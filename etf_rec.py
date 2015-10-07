__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
import csv
import itertools

logger = logging.getLogger(__name__)


def qry_get_recommendation(tt_name):
    return '''
    WITH recomm as (
    select rank() over (partition by tt.StockCode order by irec.DateFrom) as luan, tt.stockCode, r.Recommendation, irec.DateFrom
    from {tt_name} tt
    left join vewEquities ve on tt.stockCode = ve.stockCode
    left join tblInvestmentRecommendation irec on ve.StockID = irec.InvestmentID
    left join tblRecommendation r on irec.RecommendationID = r.RecommendationID
    )
    select stockcode, Recommendation, DateFrom from recomm
    where luan=1
    '''.format(tt_name=tt_name)

def get_recommendation(db, etf_stock_list, output):
    csvreader = csv.reader(etf_stock_list)
    stocks = [x for x in csvreader]

    create_table_qry = '''
    create table {table_name} (
        stockCode varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS
    )
    '''
    tt_name = TempTable.create_from_data(db, stocks, create_table_qry)
    logger.info(tt_name)

    rows = db.get_data(qry_get_recommendation(tt_name))

    csvwriter = csv.writer(output, lineterminator='\n')
    csvwriter.writerow(['StockCode', 'Recommendation', 'DateFrom'])
    csvwriter.writerows(rows)


def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', default=r'MEL-TST-001\WEBSQL', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-i', '--input', help=r'ETF Stock list', required=True, type=argparse.FileType('r'))
    parser.add_argument('-o', '--output', help=r'CSV Output file', required=True, type=argparse.FileType('w'))
    parser.add_argument('-v', '--verbose', action='count', default=0)
    a = parser.parse_args()

    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)

    connection_string1 = r'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};' \
                         'Trusted_Connection=yes;'.format(server=a.server, database=a.database)

    db = DB.from_connection_string(connection_string1)
    if a.verbose > 1:
        db.debug = True

    get_recommendation(db, a.input, a.output)



if __name__ == '__main__':
    consoleUI()
