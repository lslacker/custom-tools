__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
import requests
import helper
import csv
from bs4 import BeautifulSoup
import datetime
from collections import namedtuple

logger = logging.getLogger(__name__)

WEEKDAY = ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY']

weekday = {day_idx: day for day_idx, day in zip(range(7), WEEKDAY)}


@helper.debug
def get_qry_stock_check():
    return '''
    select ve.*
    from vewEquities ve
    where not exists (select 1
                      from ExternalData..tblTradeData td
                      where ve.stockCode = td.investmentCode and date between ? and getdate())
    '''

    return dict(investment_id=data)


Rule = namedtuple('Rule', ['element', 'attr', 'default'])


class StockCheck:
    rules = []

    @staticmethod
    def add_rule(rule):
        StockCheck.rules.append(rule)

    @staticmethod
    def check_stock(stock_code):
        url = 'http://www.bloomberg.com/quote/{stock_code}:AU'.format(stock_code=stock_code.upper())
        logger.info('Checking url={url}'.format(url=url))
        r = requests.get(url)
        text = r.text
        if '"securityType":"UNKNOWN"' in text:
            return ['UNKNOWN Security Type']

        soup = BeautifulSoup(text, 'html.parser')
        for rule in StockCheck.rules:
            divs = soup.find_all(rule.element, attrs=rule.attr)
            if divs:
                return [e.text.strip().replace('\n', ';') or rule.default for e in divs]



def check_stock(db, output):
    logger.info(weekday)
    today = datetime.date.today()
    #day_of_week = weekday.get(today.weekday())
    #if day_of_week == 'MONDAY':
    lastweek_from_today = today - datetime.timedelta(days=7)

    rule1 = Rule('div', {'class': 'market-status-message show'}, 'Try to add more rule check')
    rule2 = Rule('div', {'class': 'price-datetime'}, 'No data')

    StockCheck.add_rule(rule1)
    StockCheck.add_rule(rule2)

    rows = db.get_data(get_qry_stock_check(), [lastweek_from_today])

    data = {row[1]: StockCheck.check_stock(row[1])for row in rows}

    csvwriter = csv.writer(output, lineterminator='\n')
    csvwriter.writerow(['StockCode', 'Message'])

    for data, message in data.items():
        logger.info(message)
        if message:
            csvwriter.writerow([data, ','.join(message)])
        else:
            csvwriter.writerow([data, ''])


def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', default=r'MEL-TST-001\WEBSQL', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-o', '--output', required=True, type=argparse.FileType('w'), help='Output file')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('--dry-run', help='won\'t commit any changes', action='store_true')

    a = parser.parse_args()

    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)

    connection_string1 = r'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};' \
                         'Trusted_Connection=yes;'.format(server=a.server, database=a.database)

    db = DB.from_connection_string(connection_string1)
    if a.verbose > 1:
        db.debug = True

    check_stock(db, a.output)

    if not a.dry_run:
        logger.info('Commit changes')
        db.commit()
    else:
        logger.info('All changes did not commit')

if __name__ == '__main__':
    consoleUI()
