__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
from reader import ExcelReader
import datetime
from itertools import repeat

logger = logging.getLogger(__name__)


def get_investment_id(db, investment_code):

    return db.get_one_value('''
        select stockID
        from vewEquities
        where stockCode=?
        ''', investment_code) if get_investment_id else None


def delists(db, investment_id, investment_code, investment_status_id):
    investment_ids = investment_id.split(',') if investment_id else repeat(None)
    investment_codes = investment_code.split(',') if investment_code else repeat(None)
    params = (zip(investment_ids, investment_codes, repeat(investment_status_id)))
    return any([delist(db, *param) for param in params])


def delist(db, investment_id, investment_code, investment_status_id):
    investment_id = investment_id or get_investment_id(db, investment_code)

    data_dict = locals()
    del data_dict['db']
    logger.info('{}'.format(investment_code))
    del data_dict['investment_code']

    data_dict = ['@{k}={v!r}'.format(k=k.replace('_', ''), v=v) for k, v in data_dict.items()]

    proc_query = '''
    exec Lonsec.dbo.prcInvestmentVariablesPut {params}
    '''.format(params=','.join(data_dict))
    logger.info(proc_query)

    count = db.execute(proc_query)

    # count is always -1, does not make sense to return it???
    return count



def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', default=r'MEL-TST-001\WEBSQL', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('--investment-status-id', help='Investment Status ID. Default: 3 (closed)', type=int, default=3)
    parser.add_argument('--dry-run', help='An excel file (normally from Jen Lee)', action='store_true')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--investment-code', help='Investment Code aka Stock Code')
    group.add_argument('--investment-id', help='Investment ID aka Stock ID')

    a = parser.parse_args()

    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)

    connection_string1 = r'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};' \
                         'Trusted_Connection=yes;'.format(server=a.server, database=a.database)

    db = DB.from_connection_string(connection_string1)
    if a.verbose > 1:
        db.debug = True

    logger.info(delists(db, a.investment_id, a.investment_code, a.investment_status_id))

    if not a.dry_run:
        logger.info('Commit changes')
        db.commit()
    else:
        logger.info('All changes did not commit')

if __name__ == '__main__':
    consoleUI()
