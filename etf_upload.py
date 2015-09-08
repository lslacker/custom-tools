__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
import helper
import datetime
import itertools

logger = logging.getLogger(__name__)

@helper.debug
def qry_delete_codes_in_externaldata_tblGrowthSeries(tt_name, data_provider):
    return '''
    delete gs
    from ExternalData.dbo.tblGrowthSeries  gs
    inner join (select distinct code, date from {tt_name}) tt on gs.externalcode = tt.code and gs.date = tt.date
    where gs.dataproviderid = {data_provider}
    '''.format(tt_name=tt_name, data_provider=data_provider)

@helper.debug
def qry_add_codes_in_externaldata_tblGrowthSeries(tt_name, data_provider):
    return '''
    insert into ExternalData.dbo.tblGrowthSeries (ExternalCode, Date, Value, ValueExclDiv, DataProviderID, DateUpdated)
    select distinct Code, Date, Value, 0, {data_provider}, getdate()
    from {tt_name}
    '''.format(tt_name=tt_name, data_provider=data_provider)

@helper.debug
def qry_check_codes_not_exist_in_tblStock(tt_name):
    return '''
    select distinct code
    from {tt_name} a
    where not exists (select 1 from vewEquities b where a.code = b.stockCode)
    '''.format(tt_name=tt_name)

@helper.debug
def qry_check_codes_not_exist_in_table(table_name, tt_name, data_provider, key_field):
    return '''
    select distinct code
    from {tt_name} a
    where not exists
         (select 1 from {table_name} b where a.code = b.{key_field}
            and b.dataproviderid = {data_provider})
    '''.format(tt_name=tt_name, data_provider=data_provider, table_name=table_name, key_field=key_field)

@helper.debug
def qry_update_last_date_of_month(tt_name):
    return '''
    update {tt_name}
    set [date] = ExternalData.dbo.fnLastDayOfMonth(Date)
    where [date] <> ExternalData.dbo.fnLastDayOfMonth(Date)
    '''.format(tt_name=tt_name)

@helper.debug
def qry_refresh_data_table_GrowthSeries(table_name, tt_name, data_provider, key_field):
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
    '''.format(tt_name=tt_name, data_provider=data_provider, table_name=table_name, key_field=key_field)

@helper.debug
def qry_add_new_data_table(table_name, to_be_added_stock_codes, data_provider, upload_type, key_field_code, key_field_id):

    view_name = 'viewEquities' if 'investment' in upload_type else 'vewISF_Benchmark'
    field = 'stock' if 'investment' in upload_type else upload_type
    field1 = 'stock' if 'investment' in upload_type else 'Alternative'

    return '''
        declare @stockcodes VARCHAR(MAX) = '{codes}'
        insert into {table_name}({key_field_id}, DataProviderID, {key_field_code}, IsUsedForGrowthSeries)
        select distinct s.{field}ID,  {data_provider} as DataProviderID, s.{field1}Code, 1 as IsUsedForGrowthSeries
        from {view_name} s
        inner join [dbo].[f_csvToTableVarchar](@stockcodes) t on s.{field1}Code = t.ID
    '''.format(codes=','.join(to_be_added_stock_codes), data_provider=data_provider, field=field, field1=field1
               , table_name=table_name, view_name=view_name, key_field_code=key_field_code, key_field_id=key_field_id)

@helper.debug
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


def upload(db, excel_file, sheet_name_or_idx, data_provider, upload_type):

    gs_table_name, table_name, key_field_code, key_field_id \
        = 'tbl{upload_type}GrowthSeries tbl{upload_type}Code {upload_type}Code {upload_type}Id'\
          .format(upload_type=upload_type).split(' ')

    # Import excel file into temp table
    tt_name = helper.upload_excel_to_tempdb(db, excel_file, sheet_name_or_idx)

    # back up ExternalData.dbo.tblGrowthSeries
    backup_table(db, 'ExternalData.dbo.tblGrowthSeries')

    count = db.execute(qry_delete_codes_in_externaldata_tblGrowthSeries(tt_name, data_provider))
    logger.info('{} rows deleted in ExternalData..tblGrowthSeries'.format(count))

    count = db.execute(qry_add_codes_in_externaldata_tblGrowthSeries(tt_name, data_provider))
    logger.info('{} rows inserted in ExternalData..tblGrowthSeries'.format(count))

    rows = db.get_data(qry_check_codes_not_exist_in_tblStock(tt_name))
    if rows and 'investment' in upload_type:
        for row in rows:
            logger.info('{} does not exist in tblStock (due to trigger, capture only 1 message'.format(row.code))
        raise Exception('There are stock codes not exist in tblStock')

    logger.info('Before inserting data to {gs_table_name} from ExternalData..tblGrowthSeries'
                .format(gs_table_name=gs_table_name))
    rows = db.get_data('''
    select top 1 igs.*
    from {gs_table_name} igs
    inner join {table_name} ic on igs.{upload_type}id = ic.{upload_type}id
    where ic.{key_field}=(select top 1 code from {tt_name} order by newid())
    and ic.isUsedForGrowthSeries= 1
    order by igs.[date] desc
    '''.format(tt_name=tt_name, table_name=table_name, key_field=key_field_code
               , upload_type=upload_type, gs_table_name=gs_table_name))
    logger.info(rows)

    backup_table(db, gs_table_name)
    db.execute(qry_refresh_data_table_GrowthSeries(table_name, tt_name, data_provider, key_field_code))

    logger.info('After inserting data to {}'.format(gs_table_name))
    try:
        rows = db.get_data('''
        select top 1 * from {gs_table_name}
        where {upload_type}id=?
        order by [date] desc
        '''.format(upload_type=upload_type, gs_table_name=gs_table_name), rows[0][1])
        logger.info(rows)
    except IndexError:
        logger.info("data should be All new (INSERT INSTEAD OF UPDATE)")

    # now insert new codes if exist
    to_be_added_stock_codes = db.get_data(qry_check_codes_not_exist_in_table(table_name, tt_name
                                                                             , data_provider, key_field_code))
    if to_be_added_stock_codes:
        to_be_added_stock_codes = [row.code for row in to_be_added_stock_codes]
        logger.info('{} do not exist in {}, need to add'.format(table_name, to_be_added_stock_codes, table_name))
        count = db.execute(qry_add_new_data_table(table_name, to_be_added_stock_codes, data_provider, upload_type
                                                  , key_field_code, key_field_id))
        logger.info("{} has been added in tbl{type}Code"
                    .format(count, type='stock' if 'investment' in upload_type else upload_type))

        rows = db.get_data('''
        select top 1 igs.*
        from {gs_table_name} igs
        inner join {table_name} ic on igs.{key_field_id} = ic.{key_field_id}
        where ic.{key_field_code} = ?
        and ic.isUsedForGrowthSeries= 1
        order by igs.[date] desc
        '''.format(gs_table_name=gs_table_name, key_field_code=key_field_code, table_name=table_name
                   , key_field_id=key_field_id), to_be_added_stock_codes[-1])
        logger.info(rows)
        # can not do asset because trigger return 0 :-(
        # assert len(to_be_added_stock_codes) == count

    if 'investment' in upload_type:
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
    parser.add_argument('-i', '--input', help='An excel file from James Chang)', required=True)
    parser.add_argument('--upload-type', help='Investment Growth or Benchmark Growth'
                        , choices=['benchmark', 'investment']
                        , required=True)
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

    tt_name = upload(db, a.input, a.sheet, a.data_provider, a.upload_type)
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
