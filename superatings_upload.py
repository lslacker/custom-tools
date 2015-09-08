__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
import helper
import datetime
import itertools

logger = logging.getLogger(__name__)


def debug(fn):

    def wraps(*args, **kwargs):
        query = fn(*args, **kwargs)
        logger.info('\n{}:\n{}'.format(fn.__name__, query))
        return query

    return wraps


def qry_delete_codes_in_externaldata_tblGrowthSeries(tt_name, data_provider):
    return '''
    delete gs
    from ExternalData.dbo.tblGrowthSeries  gs
    inner join (select distinct code, date from {tt_name}) tt on gs.externalcode = tt.code and gs.date = tt.date
    where gs.dataproviderid = {data_provider}
    '''.format(tt_name=tt_name, data_provider=data_provider)


def qry_add_codes_in_externaldata_tblGrowthSeries(tt_name, data_provider):
    return '''
    insert into ExternalData.dbo.tblGrowthSeries (ExternalCode, Date, Value, ValueExclDiv, DataProviderID, DateUpdated)
    select distinct Code, Date, Value, 0, {data_provider}, getdate()
    from {tt_name}
    '''.format(tt_name=tt_name, data_provider=data_provider)


def qry_check_codes_not_exist_in_tblStock(tt_name):
    return '''
    select distinct code
    from {tt_name} a
    where not exists (select 1 from vewEquities b where a.code = b.stockCode)
    '''.format(tt_name=tt_name)


def qry_check_codes_not_exist_in_tblBenchmarkCode(tt_name, data_provider):
    return '''
    select distinct code
    from {tt_name} a
    where not exists
         (select 1 from tblBenchmarkCode b where a.code = b.benchmarkCode
            and b.dataproviderid = {data_provider})
    '''.format(tt_name=tt_name, data_provider=data_provider)


def qry_update_last_date_of_month(tt_name):
    return '''
    update {tt_name}
    set [date] = ExternalData.dbo.fnLastDayOfMonth(Date)
    where [date] <> ExternalData.dbo.fnLastDayOfMonth(Date)
    '''.format(tt_name=tt_name)


def qry_refresh_data_in_table(table_name, tt_name, data_provider, key_field):
    return '''
        update ic
        set IsUsedForGrowthSeries = 0
        from {table_name} ic
           join (select distinct code from {tt_name}) tt on ic.{key_field} = tt.Code
        where ic.IsUsedForGrowthSeries = 1
        ;
        update ic
        set IsUsedForGrowthSeries = 1
        from {table_name} ic
           join (select distinct code from {tt_name}) tt on ic.{key_field} = tt.Code
        where ic.dataproviderid = {data_provider}
    '''.format(table_name=table_name, tt_name=tt_name, data_provider=data_provider, key_field=key_field)


def qry_add_new_data_tblInvestmentGrowthSeries(to_be_added_stock_codes, data_provider):
    return '''
        declare @stockcodes VARCHAR(MAX) = '{codes}'
        insert into tblInvestmentCode(InvestmentID, DataProviderID, InvestmentCode, IsUsedForGrowthSeries)
        select distinct s.stockID,  {data_provider} as DataProviderID, s.stockCode, 1 as IsUsedForGrowthSeries
        from tblstock s
        inner join [dbo].[f_csvToTableVarchar](@stockcodes) t on s.stockCode = t.ID
    '''.format(codes=','.join(to_be_added_stock_codes), data_provider=data_provider)


def qry_regenerate_report(tt_name):
    return '''
    update tblInvestmentReport
    set Regenerate = 1
    where reportID in (11,24)
    and InvestmentID in (select StockID from Lonsec.dbo.vewEquities
                         where StockCode in (select distinct code from {tt_name}))
    and IsActive = 1 and Regenerate = 0
    '''.format(tt_name=tt_name)


def backup_table(db, tt_name):
    logger.info('Backing up table {}'.format(tt_name))

    today = datetime.date.today()
    today = today.strftime('%Y%m%d')
    new_tt_name = '{tt_name}_{date}'.format(tt_name=tt_name, date=today)

    if not db.sp_columns(new_tt_name):
        # if table does not exist
        logger.info('Creating {new_tt_name}'.format(new_tt_name=new_tt_name))
        db.execute('select * into {new_tt_name} from {tt_name}'.format(tt_name=tt_name, new_tt_name=new_tt_name))
    else:
        logger.info('Table {new_tt_name} already exists'.format(new_tt_name=new_tt_name))


def upload_benchmark(db, excel_file, sheet_name_or_idx, data_provider):
    # Import excel file into temp table

    tt_name = helper.upload_excel_to_tempdb(db, excel_file, sheet_name_or_idx)
    db.execute(qry_update_last_date_of_month(tt_name))

    # back up ExternalData.dbo.tblGrowthSeries
    backup_table(db, 'ExternalData.dbo.tblGrowthSeries')

    count = db.execute(qry_delete_codes_in_externaldata_tblGrowthSeries(tt_name, data_provider))
    logger.info('{} deleted from ExternalData.dbo.tblGrowthSeries'.format(count))

    count = db.execute(qry_add_codes_in_externaldata_tblGrowthSeries(tt_name, data_provider))
    logger.info('{} added into ExternalData.dbo.tblGrowthSeries'.format(count))
    assert count == len(tt_name)

    logger.info('Before inserting data to tblBenchmarkGrowthSeries from ExternalData..tblGrowthSeries')
    rows = db.get_data('''
    select top 1 igs.*
    from tblBenchmarkGrowthSeries igs
    inner join tblBenchmarkCode ic on igs.benchmarkID = ic.benchmarkID
    where ic.BenchmarkCode=(select top 1 code from {tt_name} order by newid())
    and ic.isUsedForGrowthSeries= 1
    order by igs.[date] desc
    '''.format(tt_name=tt_name))
    logger.info(rows)

    count = db.execute(qry_refresh_data_in_table('tblBenchmarkCode', tt_name, data_provider, 'benchmarkCode'))
    # assert count == len(tt_name)
    try:
        logger.info('After inserting data to tblBenchmarkGrowthSeries'.format(rows[0][1]))
        rows = db.get_data('''
        select top 1 * from tblBenchmarkGrowthSeries
        where benchmarkid=?
        order by [date] desc
        ''', rows[0][1])
        logger.info(rows)
    except:
        logger.info("New or Error")

    # now insert new codes if exist
    to_be_added_benchmark_codes = db.get_data(qry_check_codes_not_exist_in_tblBenchmarkCode(tt_name, data_provider))

    if to_be_added_benchmark_codes:
        to_be_added_benchmark_codes = [row.code for row in to_be_added_benchmark_codes]
        logger.info('{} do not exist in tblInvestmentCode, need to add'.format(to_be_added_benchmark_codes))
        logger.info('Need to be implemented')
    else:
        logger.info('There is no new benchmark codes')


def upload_investment(db, excel_file, sheet_name_or_idx, data_provider):
    # Import excel file into temp table

    def qry_insert_straight_to_tblInvestmentGrowthSeries(tt_name):
        # always Lonsec data provider = 1
        return '''
        insert into tblInvestmentGrowthSeries (InvestmentID, Date, Value, ValueExDiv, IsLonsecData)
        select code, [Date], [Value], 0, 1
        from {tt_name}
        '''.format(tt_name=tt_name)

    def qry_delete_all_existing_data_this_month(tt_name):
        return '''
        delete igs
        from [tblInvestmentGrowthSeries] igs
        join {tt_name} tt
             on     tt.Code = igs.InvestmentID
                and igs.Date = tt.date
        '''.format(tt_name=tt_name)

    def qry_check_all_existing_data_previous_month(tt_name):
        return '''
        select count(*)
        from [tblInvestmentGrowthSeries] igs
        join {tt_name} tt
             on     tt.Code = igs.InvestmentID
                and igs.Date = cast(DATEADD(MONTH, DATEDIFF(MONTH, -1, cast(tt.date as date))-1, -1) as date)
        '''.format(tt_name=tt_name)

    tt_name = helper.upload_excel_to_tempdb(db, excel_file, sheet_name_or_idx)
    db.execute(qry_update_last_date_of_month(tt_name))

    count = db.get_one_value(qry_check_all_existing_data_previous_month(tt_name))
    assert count == len(tt_name)

    # back up ExternalData.dbo.tblGrowthSeries
    backup_table(db, 'tblInvestmentGrowthSeries')

    count = db.execute(qry_delete_all_existing_data_this_month(tt_name))
    logger.info('{} deleted from tblInvestmentGrowthSeries'.format(count))

    count = db.execute(qry_insert_straight_to_tblInvestmentGrowthSeries(tt_name))
    logger.info('{} inserted into tblInvestmentGrowthSeries'.format(count))


def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', default=r'MEL-TST-001\WEBSQL', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('-i', '--input', help='An excel file (normally from Jen Lee)', required=True)
    parser.add_argument('--upload-type', help='Investment Growth or Benchmark Growth'
                        , choices=['benchmark', 'investment']
                        , required=True)
    parser.add_argument('--sheet', help='Sheet Name or Sheet Index', required=True)
    parser.add_argument('--data-provider', help='ETF: 4, Lonsec: 1, more at tblDataProvider', default=8, required=True)
    parser.add_argument('--dry-run', action='store_true')

    a = parser.parse_args()

    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)

    connection_string = r'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};' \
                         'Trusted_Connection=yes;'.format(server=a.server, database=a.database)

    db = DB.from_connection_string(connection_string)

    if a.verbose > 1:
        db.debug = True

    if a.upload_type == 'benchmark':
        upload_benchmark(db, a.input, a.sheet, a.data_provider)
    else:
        upload_investment(db, a.input, a.sheet, a.data_provider)
    if not a.dry_run:
        logger.info('Commit changes')
        db.commit()
    else:
        logger.info('All changes did not commit')

if __name__ == '__main__':
    consoleUI()
