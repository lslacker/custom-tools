__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
from reader import ExcelReader
import datetime
import decimal

logger = logging.getLogger(__name__)


def get_stock_id(db, stock_code):
    return db.get_one_value('''
    select stockID
    from vewEquities
    where stockCode=?
    ''', stock_code)


def get_latest_investmentlistinvestmentid(db, investment_list_id, stock_id):
    query = '''
    select investmentlistinvestmentID, weight
    from (
        select rank() over (order by datefrom desc) as luan_r, *
        from tblInvestmentListInvestment
        where investmentListID={} and investmentid={} and dateto = '2079-06-06'
    ) t
    where luan_r = 1
    '''.format(investment_list_id, stock_id)

    data = db.get_data(query)

    if len(data) > 1:
        raise Exception("> 2")
    try:
        data = data[0]
    except:
        return None, None
    return data[0], data[1]


def add_weight(db, investmentlistid, investmentid, weight):

    datefrom = datetime.date.today()
    datefrom = datefrom.strftime('%Y-%m-%d')
    logger.info(datefrom)

    query = '''
    prcInvestmentListInvestmentPut @investmentlistinvestmentid=0
    , @investmentListID={investmentlistid}
    , @InvestmentID={investmentid}
    , @Weight={weight}
    , @DateFrom={datefrom!r}, @DateTo='2079-06-06'
    , @dateauthorised = {datefrom!r}
    , @authorisedby = 'LMai'
    '''.format(investmentlistid=investmentlistid, investmentid=investmentid, weight=weight, datefrom=datefrom)

    logger.info(query)
    count = db.execute(query)

    logger.info('{} added'.format(count))

def expire_current_one(db, investmentlistinvestmentid):

    dateto = datetime.date.today() - datetime.timedelta(days=1)
    dateto = dateto.strftime('%Y-%m-%d')

    query = '''
    prcInvestmentListInvestmentPut @investmentlistinvestmentid={investmentlistinvestmentid}
    , @DateTo={dateto!r}
    '''.format(investmentlistinvestmentid=investmentlistinvestmentid, dateto=dateto)

    logger.info(query)
    count = db.execute(query)
    logger.info('{} added'.format(count))


def add(db, investment_list_id, weight):

    weights = [x.split(' ') for x in weight]

    for new_weight, stock_code in weights:
        stock_id = get_stock_id(db, stock_code)
        investmentlistinvestmentid, current_weight = get_latest_investmentlistinvestmentid(db, investment_list_id, stock_id)
        try:
            new_weight = current_weight + decimal.Decimal(new_weight)
        except:
            pass

        add_weight(db, investment_list_id, stock_id, new_weight)

        if investmentlistinvestmentid:
            expire_current_one(db, investmentlistinvestmentid)
        else:
            logger.info('Stock Code {} is new'.format(stock_code))



def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', default=r'MEL-TST-001\WEBSQL', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('--investment-list-id', help='Investment List ID', type=int, required=True)
    parser.add_argument('--weight', help='Model Weight ', required=True, nargs='+')
    parser.add_argument('--dry-run', help='Run without commit changes', action='store_true')

    a = parser.parse_args()

    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)

    connection_string1 = r'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};' \
                         'Trusted_Connection=yes;'.format(server=a.server, database=a.database)

    db = DB.from_connection_string(connection_string1)
    if a.verbose > 1:
        db.debug = True
    logger.info(a)
    add(db, a.investment_list_id, a.weight)

    if not a.dry_run:
        logger.info('Commit changes')
        db.commit()
    else:
        logger.info('All changes did not commit')

if __name__ == '__main__':
    consoleUI()
