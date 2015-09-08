__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
from reader import ExcelReader
import datetime

logger = logging.getLogger(__name__)


def add(db, stock_code, stock_name, exchange_name, sector, investment_type_id, show_on_web, investment_status_id, investment_id):
    data_dict = locals()
    del data_dict['db']
    data_dict = ['@{k}={v!r}'.format(k=k.replace('_', ''), v=v) for k, v in data_dict.items() if v]

    proc_query = '''
    exec Lonsec.dbo.prcInvestmentPut {params}
    '''.format(params=','.join(data_dict))
    logger.info(proc_query)

    rows = db.get_data(proc_query)

    raise_error = False
    try:
        next(db)
        raise_error = True   # should not have another set of data
    except:
        logger.info('InvestmentID (StockID): {}'.format(rows[0][0]))

    if raise_error:
        raise Exception('Should not need to create new sector, please check your sector again')

def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', default=r'MEL-TST-001\WEBSQL', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('--stock-code', help='Stock Code', required=True)
    parser.add_argument('--stock-name', help='Stock Name', required=True)
    parser.add_argument('--exchange-name', help='Exchange Name', required=True)
    parser.add_argument('--sector', help='Sector Name', required=True)
    parser.add_argument('--investment-id', help='Existing investment id (if any)', type=int)
    parser.add_argument('--investment-type-id', help='Sector Name', type=int, required=True)
    parser.add_argument('--show-on-web', help='Show On Web', type=int, default=1)
    parser.add_argument('--investment-status-id', help='Investment Status ID', type=int, default=1)
    parser.add_argument('--dry-run', help='An excel file (normally from Jen Lee)', action='store_true')

    a = parser.parse_args()

    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)

    connection_string1 = r'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};' \
                         'Trusted_Connection=yes;'.format(server=a.server, database=a.database)

    db = DB.from_connection_string(connection_string1)
    if a.verbose > 1:
        db.debug = True

    add(db, a.stock_code, a.stock_name, a.exchange_name, a.sector
        , a.investment_type_id, a.show_on_web, a.investment_status_id, a.investment_id)

    if not a.dry_run:
        logger.info('Commit changes')
        db.commit()
    else:
        logger.info('All changes did not commit')

if __name__ == '__main__':
    consoleUI()
