__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
import collections
import csv

logger = logging.getLogger(__name__)


def qry_get_all_fe_current_gs():
    return '''
        select *
        from tblInvestmentCode
        where DataProviderId=3 and IsUsedForGrowthSeries=1
    '''

def qry_get_latest_gs(tt_name):
    return '''
    select *
    from (
        select rank() over (partition by ic.investmentcode order by date desc) as luan_r
        , ic.investmentCode, igs.Date, igs.Value, igs.ValueExDiv, ic.investmentID
        from {tt_name} ic
        inner join tblInvestmentGrowthSeries igs on ic.InvestmentID = igs.InvestmentID
        where 0 = all (select isLonsecData from tblInvestmentGrowthSeries igs1 where igs1.investmentID = igs.investmentID)
    ) T
    --where luan_r=1
    '''.format(tt_name=tt_name)


def qry_compare_last_gs_with_external(tt_name):
    return '''
    With T as (
    select *
    --, rank() over (partition by externalcode order by date desc) as luan_r
    from ExternalData..tblGrowthSeries
    where dataproviderid=3
    )
    select tt.investmentID, tt.investmentCode, tt.Date, tt.Value, T.Value as ExternalValue
    , tt.ValueExDiv, T.ValueExclDiv as ExternalValueExDiv, fapir.ApirCode
    from {tt_name} tt
    inner join ExternalData..vewFundAPIRCode fapir on fapir.externalcode = tt.investmentcode
    left join T on T.externalCode = tt.investmentcode and T.date = tt.date
    where T.Value <> tt.Value or T.ValueExclDiv <> tt.ValueExDiv
    '''.format(tt_name=tt_name)


def double_check(db, output):
    all_fe_tt = TempTable.create_from_query(db, qry_get_all_fe_current_gs())
    all_latest_gs_tt = TempTable.create_from_query(db, qry_get_latest_gs(all_fe_tt))

    logger.info(len(all_fe_tt))
    logger.info(len(all_latest_gs_tt))

    data = db.get_data('''
    select investmentID, investmentCode from {tt_name}
    except
    select investmentID, investmentCode from {tt_name_1}
    '''.format(tt_name=all_fe_tt, tt_name_1=all_latest_gs_tt))

    no_gs_why = [row for row in data]

    logger.info('-'*40)
    logger.info('Below codes do not have current growth series')
    logger.info(no_gs_why)
    logger.info('-'*40)

    data = db.get_data(qry_compare_last_gs_with_external(all_latest_gs_tt))
    data = [row for row in data]
    dict_data = collections.defaultdict(list)
    for row in data:
        dict_data[row[0]] += [row]

    final_data = []
    for investmentid, row in dict_data.items():
        if len(row) > 1:
            logger.info(row)
            continue
        row = list(row[0])
        row = row + [row[-3] == row[-4], row[-1] == row[-2]]
        #logger.info(row)
        final_data += [row]

    csvwriter = csv.writer(output, lineterminator='\n')
    csvwriter.writerow(['investmentID', 'investmentCode', 'Date', 'Value', 'ExternalValue', 'ValueExDiv', 'ExternalValueExDiv', 'APIRCode', 'ValueDif', 'ExDivDif'])
    csvwriter.writerows(final_data)


def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('-o', '--output', type=argparse.FileType('w'), required=True)
    parser.add_argument('--dry-run', help='An excel file (normally from Jen Lee)', action='store_true')

    a = parser.parse_args()

    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)

    connection_string1 = r'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};' \
                         'Trusted_Connection=yes;'.format(server=a.server, database=a.database)

    db = DB.from_connection_string(connection_string1)
    if a.verbose > 1:
        db.debug = True

    double_check(db, a.output)

    if not a.dry_run:
        logger.info('Commit changes')
        db.commit()
    else:
        logger.info('All changes did not commit')

if __name__ == '__main__':
    consoleUI()
