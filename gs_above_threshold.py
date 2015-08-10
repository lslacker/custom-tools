__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
from reader import ExcelReader
import datetime

logger = logging.getLogger(__name__)


def import_growth_series_query(tt_name):
    return '''
        truncate table [ExternalData].dbo.ImportGrowthSeries;

        insert into [ExternalData].dbo.ImportGrowthSeries
        select
        vf.InvestmentID as Code,
        vf.InvestmentName as Name,
        gs.Date,
        gs.Value,
        gs.ValueExclDiv as ValueExDiv,
        null as CurrencyCode
        from [ExternalData].dbo.tblGrowthSeries gs
        inner join vewISF_Fund vf on gs.ExternalCode = vf.ExternalCode
        inner join {tt_name} tt on cast(tt.date as date) = gs.date and vf.InvestmentID = tt.InvestmentID
        order by vf.InvestmentID
    '''.format(tt_name=tt_name)


def merge_to_tblInvestmentGrowthSeries_query():
    return '''
        MERGE INTO tblInvestmentGrowthSeries AS target
        USING (
            --this is FE data, so IsLonsecData = 0
            select td.code, [Date], [Value], [ValueExDiv], 0 AS IsLonsecData
            from ExternalData.dbo.ImportGrowthSeries td join Lonsec.dbo.tblInvestment i on td.code = i.InvestmentID
        ) AS source
        ON source.code = target.InvestmentID and source.[Date] = target.[Date]
        WHEN MATCHED
            THEN UPDATE SET Value = source.Value, ValueExDiv = source.ValueExDiv, IsLonsecData = source.IsLonsecData
        WHEN NOT MATCHED
            THEN INSERT (InvestmentID, [Date], Value, ValueExDiv, IsLonsecData) VALUES
            (source.code, source.[Date], source.Value, source.ValueExDiv, 0)
        ;
    '''


def process(db, excel_file, dry_run):
    # Import excel file into temp table
    reader = ExcelReader(excel_file)
    rows = reader.get_data_from_sheet(0)
    tt_name = TempTable.create_from_data(db, rows, reader.create_qry)

    # Extract records in [ExternalData].dbo.tblGrowthSeries that matches InvestmentID in temp table imported above
    count = db.execute(import_growth_series_query(tt_name))

    assert len(tt_name) == count

    # back up tblInvestmentGrowthSeries
    today = datetime.date.today()
    today = today.strftime('%Y%m%d')

    if not db.sp_columns('tblInvestmentGrowthSeries_{date}'.format(date=today)):
        # if table does not exist
        logger.info('Creating tblInvestmentGrowthSeries_{date}'.format(date=today))
        db.execute('select * into tblInvestmentGrowthSeries_{date} from tblInvestmentGrowthSeries'.format(date=today))

    # get investment id from [ExternalData].dbo.tblGrowthSeries
    investment_id = db.get_one_value('select top 1 code from [ExternalData].dbo.ImportGrowthSeries')
    logger.info(investment_id)
    logger.info('Before updating')
    logger.info(db.get_data('select top 1 * from tblInvestmentGrowthSeries where investmentid=? order by [date] desc'
                            , investment_id))

    count = db.execute(merge_to_tblInvestmentGrowthSeries_query())
    logger.info("{} records updated in tblInvestmentGrowthSeries".format(count))
    logger.info('After updating')
    logger.info(db.get_data('select top 1 * from tblInvestmentGrowthSeries where investmentid=? order by [date] desc'
                            , investment_id))

    if not dry_run:
        logger.info('Commit changes')
        db.commit()
    else:
        logger.info('All changes did not commit')


def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', default=r'MEL-TST-001\WEBSQL', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('-i', '--input', help='An excel file (normally from Jen Lee)', required=True)
    parser.add_argument('--dry-run', help='An excel file (normally from Jen Lee)', action='store_true')

    a = parser.parse_args()

    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)

    connection_string1 = r'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};' \
                         'Trusted_Connection=yes;'.format(server=a.server, database=a.database)

    db = DB.from_connection_string(connection_string1)
    if a.verbose > 1:
        db.debug = True

    process(db, a.input, a.dry_run)


if __name__ == '__main__':
    consoleUI()
