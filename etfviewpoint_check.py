__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
import helper
import urllib.request
import urllib.error
import requests

logger = logging.getLogger(__name__)



def qry_get_all_recommendation_funds():
    return '''
    With T as (
        select rank() over(partition by ve.stockID order by irec.DateFrom desc) as luan_r, ve.stockID, ve.stockCode as reportURL, irec.RecommendationID, irec.DateFrom
        from vewEquities ve
        inner join fnSectorChildren(280, 1) se on ve.SectorID = se.SectorID
        inner join tblInvestmentRecommendation irec on ve.stockID = irec.InvestmentID
    )
    select T.* from T
    where luan_r = 1 and (RecommendationID not in (93, 104, 110, 102, 94, 44) and RecommendationID is not null)
    '''

def add_report(db, fund_id, report_id, report_url, analyst_id, authoriser_id):
    count = db.execute('''
        prcInvestmentReportPut @InvestmentIDCSV='{fund_id}', @reportid={report_id}, @reportURL='{report_url}'
                        ,@IsActive=1,@AnalystID={analyst_id}, @AuthoriserID={authoriser_id}
    '''.format(fund_id=fund_id, report_id=report_id, report_url=report_url, analyst_id=analyst_id, authoriser_id=authoriser_id))

    return count

def remove_report(db, report_id):
    db.execute('''
    delete from tblFundReport
    where reportID={report_id} and isActive=1
    '''.format(report_id=report_id))


def double_check(db):
    report_id = 24

    helper.backup_table(db, 'tblInvestmentReport')
    remove_report(db, report_id)

    data = db.get_data(qry_get_all_recommendation_funds())
    fund_ids = []
    for row in data:
        fund_id = row[1]
        report_url = row[2]
        if report_url:
            fund_ids += [[fund_id]]
            #logger.info('{}\t{}'.format(fund_id, report_url))
            #count = add_report(db, fund_id, report_id, report_url, analyst_id=56526, authoriser_id=56036)
            #logger.info('Adding report for {} - {}'.format(fund_id, count))
    create_table_query = '''
    create table {table_name} (
    id int
    )
    '''
    tt_name = TempTable.create_from_data(db, fund_ids, create_table_query)
    data = db.get_data('''
    select tt.ID, ve.stockCode as reportURL
    from {tt_name} tt
    left join vewEquities ve on tt.ID = ve.StockID
    left join tblInvestmentReport ir on tt.ID = ir.InvestmentID and ir.reportID={report_id} and ir.isActive=1
    where ir.reportURL is null
    '''.format(tt_name=tt_name, report_id=report_id))
    for row in data:
        fund_id = row[0]
        report_url = row[1]
        count = add_report(db, fund_id, report_id, report_url, analyst_id=56526, authoriser_id=56036)
        logger.info('Adding report for {} - {}'.format(fund_id, count))


def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('--dry-run', help='An excel file (normally from Jen Lee)', action='store_true')

    a = parser.parse_args()

    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)

    connection_string1 = r'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};' \
                         'Trusted_Connection=yes;'.format(server=a.server, database=a.database)

    db = DB.from_connection_string(connection_string1)
    if a.verbose > 1:
        db.debug = True

    double_check(db)

    if not a.dry_run:
        logger.info('Commit changes')
        db.commit()
    else:
        logger.info('All changes did not commit')

if __name__ == '__main__':
    consoleUI()
