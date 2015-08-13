__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
from reader import ExcelReader
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
    delete from ExternalData.dbo.tblGrowthSeries
    where ExternalCode in (select distinct code from {tt_name})
    --and dataproviderid = {data_provider}
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


def qry_check_codes_not_exist_in_tblInvestmentCode(tt_name, data_provider):
    return '''
    select distinct code
    from {tt_name} a
    where not exists
         (select 1 from tblInvestmentCode b where a.code = b.investmentCode
            and b.dataproviderid = {data_provider})
    '''.format(tt_name=tt_name, data_provider=data_provider)


def qry_update_last_date_of_month(tt_name):
    return '''
    update {tt_name}
    set [date] = ExternalData.dbo.fnLastDayOfMonth(Date)
    where [date] <> ExternalData.dbo.fnLastDayOfMonth(Date)
    '''.format(tt_name=tt_name)


def qry_refresh_data_tblInvestmentGrowthSeries(tt_name, data_provider):
    return '''
        update ic
        set IsUsedForGrowthSeries = 0
        from tblInvestmentCode ic
           join (select distinct code from {tt_name}) tt on ic.investmentCode = tt.Code
        where ic.IsUsedForGrowthSeries = 1
        ;
        update ic
        set IsUsedForGrowthSeries = 1
        from tblInvestmentCode ic
           join (select distinct code from {tt_name}) tt on ic.investmentCode = tt.Code
        where ic.dataproviderid = {data_provider}
    '''.format(tt_name=tt_name, data_provider=data_provider)


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


def upload(db, excel_file, sheet_name_or_idx, data_provider):
    # Import excel file into temp table

    def get_data_from_excel():
        reader = ExcelReader(excel_file)
        rows = reader.get_data_from_sheet(sheet_name_or_idx)
        return rows, reader.create_qry

    tt_name = TempTable.create_from_data(db, *get_data_from_excel())

    # Extract records in [ExternalData].dbo.tblGrowthSeries that matches InvestmentID in temp table imported above
    date_value = db.get_one_value('select top 1 [date] from {}'.format(tt_name))

    if not isinstance(date_value, datetime.datetime):
        if isinstance(date_value, str):
            logger.info('Convert varchar to datetime for [date] column')

            # update table to yyyy-mm-dd format before convert to datetime type
            db.execute('''
                update {}
                set [date]=right([date],4)+'-'+SUBSTRING([date], 4, 2) + '-' + left([date],2)
            '''.format(tt_name))
        elif isinstance(date_value, float):
            logger.info('Convert float to datetime for [date] column')
            # SQL Server counts its dates from 01/01/1900 and Excel from 12/30/1899 = 2 days less.
            # update table to yyyy-mm-dd format before convert to datetime type
            db.execute('''
                alter table {tt_name}
                alter column [date] varchar(20)
            '''.format(tt_name=tt_name))

            db.execute('''
                update {tt_name}
                set date=cast(date - 2 as datetime)
            '''.format(tt_name=tt_name))

        db.execute('''
            alter table {}
            alter column [date] date
        '''.format(tt_name))

    db.execute(qry_update_last_date_of_month(tt_name))

    # back up ExternalData.dbo.tblGrowthSeries
    backup_table(db, 'ExternalData.dbo.tblGrowthSeries')

    count = db.execute(qry_delete_codes_in_externaldata_tblGrowthSeries(tt_name, data_provider))
    logger.info('{} rows deleted in ExternalData..tblGrowthSeries'.format(count))

    count = db.execute(qry_add_codes_in_externaldata_tblGrowthSeries(tt_name, data_provider))
    logger.info('{} rows inserted in ExternalData..tblGrowthSeries'.format(count))

    rows = db.get_data(qry_check_codes_not_exist_in_tblStock(tt_name))
    if rows:
        for row in rows:
            logger.info('{} does not exist in tblStock (due to trigger, capture only 1 message'.format(row.code))
        raise Exception('There are stock codes not exist in tblStock')

    logger.info('Before inserting data to tblInvestmentGrowthSeries from ExternalData..tblGrowthSeries')
    rows = db.get_data('''
    select top 1 igs.*
    from tblInvestmentGrowthSeries igs
    inner join tblInvestmentCode ic on igs.investmentid = ic.investmentid
    where ic.investmentcode=(select top 1 code from {tt_name} order by newid())
    and ic.isUsedForGrowthSeries= 1
    order by igs.[date] desc
    '''.format(tt_name=tt_name))
    logger.info(rows)

    backup_table(db, 'tblInvestmentGrowthSeries')
    db.execute(qry_refresh_data_tblInvestmentGrowthSeries(tt_name, data_provider))

    logger.info('After inserting data to tblInvestmentGrowthSeries'.format(rows[0][1]))
    rows = db.get_data('''
    select top 1 * from tblInvestmentGrowthSeries
    where investmentid=?
    order by [date] desc
    ''', rows[0][1])
    logger.info(rows)

    # now insert new codes if exist
    to_be_added_stock_codes = db.get_data(qry_check_codes_not_exist_in_tblInvestmentCode(tt_name, data_provider))
    if to_be_added_stock_codes:
        to_be_added_stock_codes = [row.code for row in to_be_added_stock_codes]
        logger.info('{} do not exist in tblInvestmentCode, need to add'.format(to_be_added_stock_codes))
        count = db.execute(qry_add_new_data_tblInvestmentGrowthSeries(to_be_added_stock_codes, data_provider))
        logger.info("{} has been added in tblStockCode".format(count))

        rows = db.get_data('''
        select top 1 igs.*
        from tblInvestmentGrowthSeries igs
        inner join tblInvestmentCode ic on igs.investmentid = ic.investmentid
        where ic.investmentcode = ?
        and ic.isUsedForGrowthSeries= 1
        order by igs.[date] desc
        ''', to_be_added_stock_codes[-1])
        logger.info(rows)
        # can not do asset because trigger return 0 :-(
        # assert len(to_be_added_stock_codes) == count

    count = db.execute(qry_regenerate_report(tt_name))
    logger.info('{} updated for regenerating report'.format(count))

    return tt_name


def upload_global(db, tt_name, data_provider):
    logger.info('Appending .ARC in code in {tt_name}'.format(tt_name=tt_name))

    count = db.execute('''
    update {tt_name}
    set code = code + '.ARC'
    where code not like '%.ARC'
    '''.format(tt_name=tt_name))

    logger.info('{} updated'.format(count))
    assert count == len(tt_name)

    count = db.execute(qry_delete_codes_in_externaldata_tblGrowthSeries(tt_name, data_provider))
    logger.info('{} rows deleted in ExternalData..tblGrowthSeries'.format(count))

    count = db.execute(qry_add_codes_in_externaldata_tblGrowthSeries(tt_name, data_provider))
    logger.info('{} rows inserted in ExternalData..tblGrowthSeries'.format(count))

    def qry_get_new_arc_stock():
        return '''
        select distinct code
        from {tt_name}
        except
        select externalcode
        from tblInvestmentReportGrowthSource
        where dataproviderid=? and externalcode like '%.ARC'
        '''.format(tt_name=tt_name)

    new_arc_stocks = list(itertools.chain(*db.get_data(qry_get_new_arc_stock(), data_provider)))

    if new_arc_stocks:
        logger.info('There are {} arc stocks needed to be added in tblInvestmentReportGrowthSource'
                    .format(len(new_arc_stocks)))
        logger.info('They are {}'.format(','.join(new_arc_stocks)))


def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', default=r'MEL-TST-001\WEBSQL', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('-i', '--input', help='An excel file (normally from Jen Lee)', required=True)
    parser.add_argument('--sheet', help='Sheet Name or Sheet Index', required=True)
    parser.add_argument('--data-provider', help='ETF: 4, Lonsec: 1, more at tblDataProvider', default=4, required=True)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--with-global', help='Use this toggle if ETF file is global', action='store_true')

    a = parser.parse_args()

    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)

    connection_string = r'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};' \
                         'Trusted_Connection=yes;'.format(server=a.server, database=a.database)

    db = DB.from_connection_string(connection_string)

    if a.verbose > 1:
        db.debug = True

    tt_name = upload(db, a.input, a.sheet, a.data_provider)
    if a.with_global:
        logger.info('{0}{1}{0}'.format('*'*10, 'NOW LOAD to ExternalData.dbo.tblGrowthSeries FOR .arc'))
        upload_global(db, tt_name, a.data_provider)

    if not a.dry_run:
        logger.info('Commit changes')
        db.commit()
    else:
        logger.info('All changes did not commit')

if __name__ == '__main__':
    consoleUI()
