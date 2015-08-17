__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
import helper
import inspect

logger = logging.getLogger(__name__)


def get_investment_id(db, stock_code):
    data = db.get_one_value('''
    select stockID as investmentID
    from vewEquities
    where stockCode=?
    ''', stock_code)

    return dict(investment_id=data)


def action(db, stock_code, investment_status_id):
    investment_id = get_investment_id(db, stock_code)
    if investment_id['investment_id']:
        data_dict = locals()
        del data_dict['db']
        del data_dict['stock_code']
        data_dict.update(investment_id)
        data_dict = ['@{k}={v!r}'.format(k=k.replace('_', ''), v=v) for k, v in data_dict.items()]
        logger.info(data_dict)
        proc_query = '''
        exec prcInvestmentVariablesPut {params}
        '''.format(params=','.join(data_dict))
        logger.info(proc_query)
        db.execute(proc_query)
    else:
        logger.info('{} is already in-active'.format(stock_code))

def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', default=r'MEL-TST-001\WEBSQL', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('--stock-codes', nargs='+', help='separate by space', required=True)
    parser.add_argument('--investment-status-id', help='Sector Name', type=int, required=True)
    parser.add_argument('--dry-run', help='won\'t commit any changes', action='store_true')

    a = parser.parse_args()

    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)

    connection_string1 = r'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};' \
                         'Trusted_Connection=yes;'.format(server=a.server, database=a.database)

    db = DB.from_connection_string(connection_string1)
    if a.verbose > 1:
        db.debug = True

    any(action(db, stock_code, a.investment_status_id) for stock_code in a.stock_codes)

    if not a.dry_run:
        logger.info('Commit changes')
        db.commit()
    else:
        logger.info('All changes did not commit')

if __name__ == '__main__':
    consoleUI()
