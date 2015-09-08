__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
import helper
import datetime
from itertools import repeat

logger = logging.getLogger(__name__)


def get_investment_id(db, investment_code):

    return db.get_one_value('''
        select stockID
        from vewEquities
        where stockCode=?
        ''', investment_code) if get_investment_id else None


def delists(db, investment_code, recommendation, investment_type_id, approved_by):
    today = datetime.date.today()
    today = today.strftime('%Y-%m-%d')
    helper.backup_table(db, 'ExternalData..tblStockEarningsDividends')
    helper.backup_table(db, 'ExternalData..tblStockEarnings')
    investment_codes = investment_code.split(',')
    investment_ids = [get_investment_id(db, x) for x in investment_codes]
    params = (zip(investment_ids, investment_codes, repeat(recommendation), repeat(investment_type_id), repeat(today), repeat(approved_by)))
    return any([delist(db, *param) for param in params])


def delist(db, investment_id, investment_code, recommendation, investment_type_id, date, approved_by):

    query = '''\
    prcInvestmentRecommendationPut @InvestmentID={investment_id}, @isActive=0
    ;
    prcInvestmentRecommendationPut @InvestmentID={investment_id}, @recommendation='{recommendation}',
                @InvestmentTypeID={investment_type_id}, @ApprovedDate='{date}',
                @FromDate='{date}',
                @ApprovedBy='{approved_by}',
                @isActive=1
    ;
    delete from ExternalData..tblStockEarningsDividends
    where StockEarningsID in (
    select StockEarningsID
    from ExternalData..tblStockEarnings
    where investmentcode='{investment_code}')
    ;
    delete from ExternalData..tblStockEarnings
    where investmentcode='{investment_code}'
    '''.format(investment_id=investment_id,
               investment_code=investment_code,
               recommendation=recommendation,
               investment_type_id=investment_type_id,
               date=date,
               approved_by=approved_by)
    for q in query.split(';'):
        logger.info('Executing:\n{}'.format(q))
        count = db.execute(q)
        logger.info(count)
    return count

def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', default=r'MEL-TST-001\WEBSQL', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('--recommendation', help='Recommendation. Check tblRecommendation for Name', required=True, default='Ceased Coverage')
    parser.add_argument('--investment-type-id', help='Investment Type ID. Default: 2 (Direct Equity)', required=True, type=int, default='2')
    parser.add_argument('--approved-by', help='Approved By', required=True)
    parser.add_argument('--dry-run', help='An excel file (normally from Jen Lee)', action='store_true')
    parser.add_argument('--investment-code', help='Investment Code aka Stock Code', required=True)


    a = parser.parse_args()

    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)

    connection_string1 = r'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};' \
                         'Trusted_Connection=yes;'.format(server=a.server, database=a.database)

    db = DB.from_connection_string(connection_string1)
    if a.verbose > 1:
        db.debug = True

    logger.info(delists(db, a.investment_code, a.recommendation, a.investment_type_id, a.approved_by))

    if not a.dry_run:
        logger.info('Commit changes')
        db.commit()
    else:
        logger.info('All changes did not commit')

if __name__ == '__main__':
    consoleUI()
