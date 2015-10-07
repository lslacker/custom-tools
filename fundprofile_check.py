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



def qry_get_investment_missing_profile():
    return '''
    With T as (
    select distinct investmentID from tblInvestmentCode
    where DataProviderID=3
    except
    select distinct investmentID from tblInvestmentReport
    where isActive=1 and reportID=11
    )
    select T.*, isNull(isff.apirCode, isff.instrumentID) as reportURL
    from T
    inner join vewISF_Fund isff on T.investmentID = isff.investmentID

    '''


def add_report(db, fund_id, report_id, report_url):
    count = db.execute('''
        prcInvestmentReportPut @InvestmentIDCSV={fund_id}, @reportid={report_id}, @reportURL='{report_url}'
                        ,@IsActive=1, @Regenerate=0
    '''.format(fund_id=fund_id, report_id=report_id, report_url=report_url))

    return count

def link_exists(urllink):
    try:
        r = requests.get(urllink, stream=True)
        return True if r.headers['content-type'] == 'application/pdf' else False
    except:
        return False

def double_check(db):
    report_id = 11
    data = db.get_data(qry_get_investment_missing_profile())

    for row in data:
        fund_id = row[0]
        report_url = row[-1]
        urllink = 'https://reports.lonsec.com.au/FP/{}'.format(report_url)

        is_link_exists = link_exists(urllink)
        if is_link_exists:
            logger.info('{}\t{}'.format(fund_id, report_url))
            count = add_report(db, fund_id, report_id, report_url)
            logger.info('Adding report for {} - {}'.format(fund_id, count))
        else:
            logger.info('Checking {} exists'.format(urllink))
            logger.info('----> NOT OK')


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
