__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
from reader import ExcelReader
import datetime
import helper

logger = logging.getLogger(__name__)


def debug(fn):

    def wraps(*args, **kwargs):
        query = fn(*args, **kwargs)
        logger.info('\n{}:\n{}'.format(fn.__name__, query))
        return query

    return wraps


##### BEGIN - TOP 10 HOLDING QUERIES ###
def qry_get_distinct_code_date(tt_name):
    return '''
    select distinct t10.ticker, t10.date, s.investmentname as name
    from {tt_name} t10
    inner join vewISF_Stock s on t10.ticker = s.investmentcode
    '''.format(tt_name=tt_name)


def qry_update_fund_name_tblUnderlyingFund(unique_tt_name):
    return '''
    UPDATE uf
    SET uf.FundName = t10.Name
    FROM ExternalData.dbo.tblUnderlyingFund uf
    JOIN {unique_tt_name} t10 ON uf.FundCode = t10.ticker
    where t10.name <> uf.FundName
    '''.format(unique_tt_name=unique_tt_name)


def qry_insert_new_fund_tblUnderlyingFund(unique_tt_name, data_provider):
    return '''
    INSERT INTO ExternalData.dbo.tblUnderlyingFund(FundCode, FundName, DataProviderID)
    select t10.ticker, t10.Name, {data_provider}
    FROM {unique_tt_name} t10
    LEFT JOIN ExternalData.dbo.tblUnderlyingFund uf ON uf.FundCode = t10.ticker
    WHERE uf.FundCode is null
    '''.format(unique_tt_name=unique_tt_name, data_provider=data_provider)


def qry_update_DateFrom_tblUnderlyingFund(unique_tt_name):
    return '''
    UPDATE uf
    SET uf.TopHoldingsDate = t10.Date
    FROM ExternalData.dbo.tblUnderlyingFund uf
    JOIN {unique_tt_name} t10 ON uf.FundCode = t10.ticker
    '''.format(unique_tt_name=unique_tt_name)


def qry_update_DateTo_of_current_holdings(unique_tt_name):
    return '''
        UPDATE th
        SET th.DateTo = DATEADD(day, - 1, t10.Date)
        FROM ExternalData.dbo.tblTopHoldings th
        JOIN ExternalData.dbo.tblUnderlyingFund uf
            ON th.UnderlyingFundID = uf.ID and th.DataProviderID = uf.DataProviderID
        JOIN {unique_tt_name} t10 ON uf.FundCode = t10.Ticker
        WHERE th.DateTo = '2079-06-06'
    '''.format(unique_tt_name=unique_tt_name)


def qry_add_new_holding_name(tt_name, data_provider):
    return '''
    INSERT INTO ExternalData.dbo.tblHolding (HoldingCode, HoldingName, DataProviderID)
    SELECT NULL, t10.NAME, {data_provider} FROM {tt_name} t10
    JOIN ExternalData.dbo.tblUnderlyingFund uf ON t10.Ticker = uf.FundCode
    LEFT JOIN ExternalData.dbo.tblHolding h ON t10.NAME = h.HoldingName
    AND h.DataProviderID = {data_provider}
    WHERE h.HoldingName IS NULL
    '''.format(tt_name=tt_name, data_provider=data_provider)


def qry_add_new_holdings(tt_name, data_provider):
    return '''
    INSERT INTO ExternalData.dbo.tblTopHoldings (UnderlyingFundID, HoldingID, Weight, DateFrom, DateTo, DataProviderID)
    SELECT uf.ID, h.ID, t10.Weight, t10.Date, '2079-06-06', {data_provider}
    FROM {tt_name} t10
    JOIN ExternalData.dbo.tblUnderlyingFund uf ON t10.Ticker = uf.FundCode
    LEFT JOIN ExternalData.dbo.tblHolding h ON t10.NAME = h.HoldingName
        AND h.DataProviderID = {data_provider}
        AND h.ID = (
            SELECT max(ID)
            FROM ExternalData.dbo.tblHolding h2
            WHERE h2.HoldingName = h.HoldingName
                AND h2.DataProviderID = {data_provider}
            )
    '''.format(tt_name=tt_name, data_provider=data_provider)


def qry_regenerate_report(tt_name, code_field='ticker'):
    return '''
    update tblInvestmentReport
    set Regenerate = 1
    where reportID = 24
    and InvestmentID in (select StockID from Lonsec.dbo.vewEquities
                         where StockCode in (select distinct {code_field} from {tt_name}))
    and IsActive = 1 and Regenerate = 0
    '''.format(tt_name=tt_name, code_field=code_field)


##### END - TOP 10 HOLDING QUERIES ###

def qry_get_attribute_id():
    return '''
    select attributeid from Lonsec..tblAttribute
    where [attribute] = ?
    '''

def qry_update_last_date_of_month(tt_name):
    return '''
    update {tt_name}
    set [date] = ExternalData.dbo.fnLastDayOfMonth(Date)
    where [date] <> ExternalData.dbo.fnLastDayOfMonth(Date)
    '''.format(tt_name=tt_name)


def qry_create_temp_table_for_attribute_name(tt_name, attribute_name, attribute_id):
    return '''
    select tt.code, s.investmentID, {attribute_id} as attribute_id,tt. [{attribute_name}] as attribute_value, tt.date
    from {tt_name} tt
    inner join vewISF_Stock s on tt.code = s.investmentCode
    where tt.[{attribute_name}] is not null and tt.[{attribute_name}] {cond}
    '''.format(tt_name=tt_name, attribute_name=attribute_name, attribute_id=attribute_id,
               cond='> 0' if attribute_id != 19 else "<>''")


def qry_update_DateTo_of_tbInvestmentAttribute(attribute_tt_name):
    return '''
        UPDATE ia
        SET ia.DateTo = DATEADD(day, - 1, tt.Date)
        FROM tblInvestmentAttribute ia
        INNER JOIN {attribute_tt_name} tt on ia.investmentID = tt.investmentID and ia.attributeid=tt.attribute_id
        WHERE getDate() between ia.DateFrom and ia.DateTo
    '''.format(attribute_tt_name=attribute_tt_name)


def qry_insert_new_investment_attributes(attribute_tt_name):
    return '''
    INSERT INTO tblInvestmentAttribute(InvestmentID, AttributeID, AttributeValue, DateFrom, DateTo, DateCreated
                                        ,CreatedBy, DateUpdated, UpdatedBy)
    SELECT InvestmentID
    ,Attribute_ID
    ,Attribute_Value
    ,date as DateFrom
    ,'2079-06-06' DateTo
    ,GETDATE() AS DateCreated
    ,SYSTEM_USER CreatedBy
    ,GETDATE() DateUpdated
    ,SYSTEM_USER AS UpdatedBy
    FROM {attribute_tt_name}
    '''.format(attribute_tt_name=attribute_tt_name)


def qry_clone_date_to_attribute_id_19(tt_name, attribute_name):
    return '''
    select *,
    CAST(DAY([date]) AS VARCHAR(2)) + ' ' + DATENAME(MM, [date]) + ' ' + CAST(YEAR([date]) AS VARCHAR(4))
        as [{attribute_name}]
    from {tt_name}
    '''.format(tt_name=tt_name, attribute_name=attribute_name)


def qry_delete_rerun_data(table_name):
    return '''
    delete from {table_name}
    where datefrom > dateto
    '''.format(table_name=table_name)


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


def upload_top10(db, excel_file, sheet_name_or_idx, data_provider):

    tt_name = helper.upload_excel_to_tempdb(db, excel_file, sheet_name_or_idx)

    unique_tt_name = TempTable.create_from_query(db, qry_get_distinct_code_date(tt_name))

    # back up ExternalData.dbo.tblUnderlyingFund
    backup_table(db, 'ExternalData.dbo.tblUnderlyingFund')
    count = db.execute(qry_update_fund_name_tblUnderlyingFund(unique_tt_name))
    logger.info('{} fund name updated in tblUnderlyingFund'.format(count))

    count = db.execute(qry_insert_new_fund_tblUnderlyingFund(unique_tt_name, data_provider))
    logger.info('{} fund name inserted in tblUnderlyingFund'.format(count))

    count = db.execute(qry_update_DateFrom_tblUnderlyingFund(unique_tt_name))
    logger.info('{} fund name updated with latest date in tblUnderlyingFund'.format(count))
    assert count == len(unique_tt_name)

    # close off any open holdings for funds where changed
    # back up ExternalData.dbo.tblTopHoldings
    backup_table(db, 'ExternalData.dbo.tblTopHoldings')
    count = db.execute(qry_update_DateTo_of_current_holdings(unique_tt_name))
    logger.info('{} updated by closing off any open holdings for funds where changed'.format(count))

    # add the new holding code
    backup_table(db, 'ExternalData.dbo.tblHolding')
    count = db.execute(qry_add_new_holding_name(tt_name, data_provider))
    logger.info('{} updated by adding the new holding code'.format(count))

    # add new holding
    count = db.execute(qry_add_new_holdings(tt_name, data_provider))
    logger.info('{} updated by adding the new holding'.format(count))
    assert count == len(tt_name)

    count = db.execute(qry_regenerate_report(tt_name))
    logger.info('{} updated for regenerating report'.format(count))

    # cleaning up junk data due to re-run
    count = db.execute(qry_delete_rerun_data('ExternalData..tblTopHoldings'))
    logger.info('{} deleted in ExternalData..tblTopHoldings due to re-run'.format(count))


def upload_cost(db, excel_file, sheet_name_or_idx):
    tt_name = helper.upload_excel_to_tempdb(db, excel_file, sheet_name_or_idx)

    # create other quries for attribute 19
    tt_name = TempTable.create_from_query(db, qry_clone_date_to_attribute_id_19(tt_name, 'Fees & Indirect Costs Date'))

    count = db.execute(qry_update_last_date_of_month(tt_name))
    logger.info('{} updated to ensure [date] column is last day of the month\n'.format(count))

    cols = db.sp_columns('{}'.format(tt_name))
    attribute_lookup = {col: db.get_one_value(qry_get_attribute_id(), col) for col in cols}
    # remove columns not matched tblAttribute
    attribute_lookup = {k: v for k, v in attribute_lookup.items() if v is not None}

    # back up tblInvestmentAttribute
    backup_table(db, 'tblInvestmentAttribute')

    for attribute_name, attribute_id in attribute_lookup.items():

        attribute_tt_name = TempTable.create_from_query(db, qry_create_temp_table_for_attribute_name(tt_name
                                                                                                    , attribute_name
                                                                                                    , attribute_id))
        logger.info('create table [{}] for attribute name [{}][{}] - Qty {}'.format(attribute_tt_name, attribute_name
                                                                                    , attribute_id
                                                                                    , len(attribute_tt_name)))

        count = db.execute(qry_update_DateTo_of_tbInvestmentAttribute(attribute_tt_name))
        logger.info('{} updated by closing off current attributes'.format(count))

        count = db.execute(qry_insert_new_investment_attributes(attribute_tt_name))
        logger.info('{} inserted into tblInvestmentAttribute'.format(count))
        assert count == len(attribute_tt_name)
        logger.info('\n')

    # cleaning up junk data due to re-run
    count = db.execute(qry_delete_rerun_data('tblInvestmentAttribute'))
    logger.info('{} deleted in tblInvestmentAttribute due to re-run'.format(count))

    count = db.execute(qry_regenerate_report(tt_name, code_field='code'))
    logger.info('{} updated for regenerating report'.format(count))


def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', default=r'MEL-TST-001\WEBSQL', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('-i', '--input', help='An excel file (normally from Jen Lee)', required=True)
    parser.add_argument('--top10', help='Top 10 sheet name', required=True)
    parser.add_argument('--cost', help='Cost sheet name', required=True)
    parser.add_argument('--data-provider', help='ETF: 4, Lonsec: 1, more at tblDataProvider', default=6, required=True)
    parser.add_argument('--dry-run', action='store_true')

    a = parser.parse_args()

    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)

    connection_string = r'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};' \
                         'Trusted_Connection=yes;'.format(server=a.server, database=a.database)

    db = DB.from_connection_string(connection_string)

    if a.verbose > 1:
        db.debug = True

    logger.info('LOADING TOP 10')
    upload_top10(db, a.input, a.top10, a.data_provider)
    logger.info('*'*40)
    logger.info('LOADING COST')
    upload_cost(db, a.input, a.cost)

    if not a.dry_run:
        logger.info('Commit changes')
        db.commit()
    else:
        logger.info('All changes did not commit')

if __name__ == '__main__':
    consoleUI()
