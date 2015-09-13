__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
from reader import ExcelReader
import datetime

logger = logging.getLogger(__name__)


def add(db, benchmark_name, alternative_code, benchmark_id, weight):
    data_dict = locals()
    del data_dict['db']
    del data_dict['weight']
    data_dict = ['@{k}={v!r}'.format(k=k.replace('_', ''), v=v) for k, v in data_dict.items() if v is not None]

    proc_query = '''
    exec Lonsec.dbo.prcBenchmarkPut {params}
    '''.format(params=','.join(data_dict))

    logger.info(proc_query)

    rows = db.get_data(proc_query)

    raise_error = False
    try:
        next(db)
        raise_error = True   # should not have another set of data
    except:
        logger.info('BenchmarkID (BenchmarkID): {}'.format(rows[0][0]))


    if raise_error:
        raise Exception('Should not need to create new sector, please check your sector again')

    def split_weight(db, x):
            weight, benchmark_code = x.split('%')
            weight = int(weight) / 100.0
            benchmark_code = benchmark_code.strip()
            benchmark_id = db.get_one_value('select benchmarkid from tblBenchmark where alternativeCode=?', benchmark_code)
            return [benchmark_id, weight]

    if weight:
        # now insert into
        weights = [[rows[0][0]] + split_weight(db, x) for x in weight]
        logger.info(weights)
        for x in weights:
            logger.info(x)
            parent_benchmark_id, benchmark_id, benchmark_weight = x

            query = '''insert into tblBenchmarkHierarchy(BenchmarkID, ParentID, Weight)
            select {benchmark_id}, {parent_benchmark_id}, {benchmark_weight}
            '''.format(benchmark_id=benchmark_id, parent_benchmark_id=parent_benchmark_id, benchmark_weight=benchmark_weight)
            count = db.execute(query)
            logger.info(count)

def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', default=r'MEL-TST-001\WEBSQL', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('--benchmark-id', help='Benchmark ID')
    parser.add_argument('--benchmark-name', help='Benchmark Name', required=True)
    parser.add_argument('--alternative-code', help='Benchmark Code', required=True)
    parser.add_argument('--weight', help='Benchmark Weight ', nargs='+')
    parser.add_argument('--dry-run', help='Run without commit changes', action='store_true')

    a = parser.parse_args()

    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)

    connection_string1 = r'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};' \
                         'Trusted_Connection=yes;'.format(server=a.server, database=a.database)

    db = DB.from_connection_string(connection_string1)
    if a.verbose > 1:
        db.debug = True

    add(db, a.benchmark_name, a.alternative_code, a.benchmark_id, a.weight)

    if not a.dry_run:
        logger.info('Commit changes')
        db.commit()
    else:
        logger.info('All changes did not commit')

if __name__ == '__main__':
    consoleUI()
