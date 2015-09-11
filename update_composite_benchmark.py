__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
from reader import ExcelReader
import datetime

logger = logging.getLogger(__name__)


def get_composite_benchmark():
    return '''
    select cast(benchmarkID as varchar) as benchmarkID, AlternativeCode from tblBenchmark
    where exists (select 1 from tblBenchmarkHierarchy where parentID=tblBenchmark.BenchmarkID)
    '''


def check_in_gs():
    return '''
    select * from ExternalData.dbo.tblGrowthSeries
    where ExternalCode=?
    '''


def update(db):
    composite_benchmarks = db.get_data(get_composite_benchmark())
    benchmarkids, alternativecodes = zip(*composite_benchmarks)

    for benchmarkid, benchmarkcode in composite_benchmarks:
        logger.info('Composite benchmark: {} - {}'.format(benchmarkid, benchmarkcode))
        data_dict = dict(BenchmarkIDCSV=benchmarkid,
                         ForceRecalculate=0,
                         CacheResults=1
                         )
        default_params = '@DateFrom=default,@DateTo=default,@StartValue=default,@DataSource=9,@ReturnRowIfMissingValue=0,'\
                         '@CurrencyCode=default,@ReturnResults=default'
        data_dict = ['@{k}={v!r}'.format(k=k.replace('_', ''), v=v) for k, v in data_dict.items()]
        proc_query = '''
        exec Lonsec.dbo.prcBenchmarkGrowthSeriesGet {params},{default_params}
        '''.format(params=','.join(data_dict), default_params=default_params)
        db.commit()
        logger.info(proc_query)
        count = db.execute(proc_query)
        logger.info(count)


def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', default=r'MEL-TST-001\WEBSQL', help='Database Server')
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

    update(db)

    if not a.dry_run:
        logger.info('Commit changes')
        db.commit()
    else:
        logger.info('All changes did not commit')

if __name__ == '__main__':
    consoleUI()
