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


def qry_update_stock_code(tt_name, fy, data_provider):
    return '''
    MERGE INTO ExternalData.dbo.tblStockEarnings AS target
    USING (select t.StockCode, e.ExchangeID, {fy} as FinancialYear, DPS * 100 as DPS
                 , [PERCENT_FRANKED]*100 as Franking, {data_provider} as DataProviderID
           from {tt_name} t
           inner join vewEquities ve on t.stockCode = ve.stockCode
           inner join tblExchange e on e.Exchange=ve.Exchange
    ) AS source
    ON source.StockCode = target.InvestmentCode and source.ExchangeID = target.ExchangeID
        and source.FinancialYear = target.FinancialYear and target.DataProviderID = source.DataProviderID
    WHEN MATCHED
        THEN UPDATE SET DPS = source.DPS, Franking = source.Franking
    WHEN NOT MATCHED
        THEN INSERT (InvestmentCode, ExchangeID, FinancialYear, DPS, Franking, DataProviderID)
        VALUES (source.StockCode, source.ExchangeID, source.FinancialYear, source.DPS, source.Franking, source.DataProviderID)
    ;
    '''.format(tt_name=tt_name, fy=fy, data_provider=data_provider)


def qry_update_model_stock_code(tt_name, fy, data_provider):
    qry = '''
    MERGE INTO ExternalData.dbo.tblStockEarnings AS target
    USING (select t.StockCode, e.ExchangeID, {fy} as FinancialYear, DPS * 100 as DPS
                 , EPS*100 as EPS, EPSGrowth * 100 as EPSGrowth, PER
                 , [PERCENT_FRANKED]*100 as Franking, {data_provider} as DataProviderID
           from {tt_name} t
           inner join vewEquities ve on t.stockCode = ve.stockCode
           inner join tblExchange e on e.Exchange=ve.Exchange
    ) AS source
    ON source.StockCode = target.InvestmentCode and source.ExchangeID = target.ExchangeID
        and source.FinancialYear = target.FinancialYear and target.DataProviderID = source.DataProviderID
    WHEN MATCHED
        THEN UPDATE SET DPS = source.DPS, Franking = source.Franking, EPS = source.EPS, EPSGrowth = source.EPSGrowth,
                        Yield = NULL, PER = source.PER
    '''.format(tt_name=tt_name, fy=fy, data_provider=data_provider)

    if data_provider == 1:
        qry += '''
        WHEN NOT MATCHED
        THEN INSERT (InvestmentCode, ExchangeID, FinancialYear, EPS, DPS, Franking, EPSGrowth, PER, DataProviderID)
        VALUES (source.StockCode, source.ExchangeID, source.FinancialYear, source.EPS, source.DPS, source.Franking, source.EPSGrowth, source.PER, source.DataProviderID)
        '''
    qry += ';'

    return qry


def qry_update_ubs_stock_code(tt_name, fy, data_provider):
    qry = '''
    MERGE INTO ExternalData.dbo.tblStockEarnings AS target
    USING (select t.StockCode, e.ExchangeID, {fy} as FinancialYear, DPS as DPS
                 , EPS as EPS
                 , [PERCENT_FRANKED]*100 as Franking, {data_provider} as DataProviderID
           from {tt_name} t
           inner join vewEquities ve on t.stockCode = ve.stockCode
           inner join tblExchange e on e.Exchange=ve.Exchange
    ) AS source
    ON source.StockCode = target.InvestmentCode and source.ExchangeID = target.ExchangeID
        and source.FinancialYear = target.FinancialYear and target.DataProviderID = source.DataProviderID
    WHEN MATCHED
        THEN UPDATE SET Franking = isNULL(source.Franking, target.Franking), EPS = isNULL(source.EPS, target.EPS), DPS = isNULL(source.DPS, target.DPS)
    WHEN NOT MATCHED
        THEN INSERT (InvestmentCode, ExchangeID, FinancialYear, EPS, DPS, Franking, DataProviderID)
        VALUES (source.StockCode, source.ExchangeID, source.FinancialYear, source.EPS, source.DPS, source.Franking, {data_provider})
    ;
    '''.format(tt_name=tt_name, fy=fy, data_provider=data_provider)

    return qry

def qry_delete_lonsec_if_ubs(tt_name, fy):
    return '''
    delete se_lonsec
    from ExternalData..tblStockEarnings se_lonsec
    inner join {tt_name} t on t.stockcode = se_lonsec.investmentcode
    inner join ExternalData..tblStockEarnings se_ubs on se_lonsec.investmentcode = se_ubs.investmentcode
    where se_lonsec.financialyear = {fy} and se_ubs.financialyear={fy} and  se_lonsec.dataproviderid=1 and se_ubs.dataproviderid=7
    '''.format(tt_name=tt_name, fy=fy)

def rename(db, tt_name, old_column, new_column):
    db.execute('''
    tempdb..sp_RENAME '{tt_name}.{old_column}', '{new_column}', 'COLUMN'
    '''.format(tt_name=tt_name, old_column=old_column, new_column=new_column))


def upload(db, excel_file, sheet_name_or_idx, data_provider, upload_type):
    # Import excel file into temp table
    tt_name = helper.upload_excel_to_tempdb(db, excel_file, sheet_name_or_idx)
    fy = helper.get_financial_year()

    logger.info(tt_name)

    if 'hybrids' in upload_type:
        rename(db, tt_name, 'Code', 'StockCode')
        rename(db, tt_name, 'Annual DPS', 'DPS')
        rename(db, tt_name, '% FRANKED', 'PERCENT_FRANKED')
    elif 'model' in upload_type:
        rename(db, tt_name, 'Stock', 'StockCode')
        rename(db, tt_name, 'FY16 DPS', 'DPS')
        rename(db, tt_name, 'FY16 EPS', 'EPS')
        rename(db, tt_name, 'FY16 EPS GROWTH', 'EPSGrowth')
        rename(db, tt_name, '% FRANKED', 'PERCENT_FRANKED')
        rename(db, tt_name, 'FY16 PER', 'PER')
    elif 'ubs' in upload_type:
        rename(db, tt_name, 'Franking', 'PERCENT_FRANKED')

    count = db.get_one_value('''
    select count(*)
    from {tt_name}
    where PERCENT_FRANKED > 1
    '''.format(tt_name=tt_name))

    if count > 0:
        db.execute('''
        update {tt_name}
        set PERCENT_FRANKED = PERCENT_FRANKED / 100
        '''.format(tt_name=tt_name))

    # back up ExternalData.dbo.tblGrowthSeries
    helper.backup_table(db, 'ExternalData.dbo.tblStockEarnings')

    rows = db.get_data('''
    select *
    from ExternalData.dbo.tblStockEarnings
    where investmentCode in (select top 2 stockCode from {tt_name}) and FinancialYear={fy}
    '''.format(tt_name=tt_name, fy=fy))
    for row in rows:
        logger.info(row)

    qry_update = qry_update_model_stock_code if 'model' in upload_type else qry_update_stock_code
    qry_update = qry_update_ubs_stock_code if 'ubs' in upload_type else qry_update
    logger.info('Data provider {}'.format(data_provider))
    count = db.execute(qry_update(tt_name, fy, data_provider))
    logger.info(count)

    if upload_type in ['model']:
        count = db.execute(qry_update(tt_name, fy, data_provider=1))
        logger.info('{} updated with 1'.format(count))
        count1 = db.execute(qry_delete_lonsec_if_ubs(tt_name, fy))
        logger.info(str(count1)+' deleted')

    logger.info('{} updated/inserted into External.dbo.tblStockEarnings'.format(count))
    rows = db.get_data('''
    select *
    from ExternalData.dbo.tblStockEarnings
    where investmentCode in (select top 2 stockCode from {tt_name}) and FinancialYear={fy}
    '''.format(tt_name=tt_name, fy=fy))

    for row in rows:
        logger.info(row)

    if count < len(tt_name):
        logger.info('Some stock codes are not available on vewEquities, they are needed to added')
        rows = db.get_data('''
        select tt_name.StockCode
        from {tt_name} tt_name
        left join vewEquities ve on tt_name.StockCode = ve.StockCode
        where ve.StockID is null
        '''.format(tt_name=tt_name))
        stocks = [row[0] for row in rows]
        logger.info('They are: {}'.format(', '.join(stocks)))

    #assert len(tt_name) == count


def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', default=r'MEL-TST-001\WEBSQL', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('-i', '--input', help='An excel file (normally from Jen Lee)', required=True)
    parser.add_argument('--upload-type', help='Investment Growth or Benchmark Growth'
                        , choices=['etf', 'hybrids', 'model', 'ubs']
                        , required=True)
    parser.add_argument('--sheet', help='Sheet Name or Sheet Index', required=True)
    parser.add_argument('--data-provider', help='ETF: 4, Lonsec: 1, more at tblDataProvider', default=8, required=True, type=int)
    parser.add_argument('--dry-run', action='store_true')

    a = parser.parse_args()

    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)

    connection_string = r'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};' \
                         'Trusted_Connection=yes;'.format(server=a.server, database=a.database)

    db = DB.from_connection_string(connection_string)

    if a.verbose > 1:
        db.debug = True

    if (a.upload_type in ['ubs', 'model']) and a.data_provider != 7:
        raise Exception('{} should have data provider 7 not {}'.format(a.upload_type, a.data_provider))

    upload(db, a.input, a.sheet, a.data_provider, a.upload_type)

    if not a.dry_run:
        logger.info('Commit changes')
        db.commit()
    else:
        logger.info('All changes did not commit')

if __name__ == '__main__':
    consoleUI()
