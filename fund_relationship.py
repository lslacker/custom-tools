__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
import helper
import urllib.request
import urllib.error
import csv

logger = logging.getLogger(__name__)



def qry_get_fund_hierarchy():
    return '''
    select
           fp.FundID as parentFundID, fc.*
    from (
            select FundID from tblFund
            where ParentID is null
         ) fp
    cross apply fnFundChildren(fp.FundID, 1) fc
    order by fp.FundID, Level
    '''

def qry_get_investment_detail(tt_name):
    return '''
    With T as (
    select rank() over(partition by ID order by datefrom desc) as luan_r, fr.FundRecommendationID, f.ID, fr.FundID, r.Recommendation, fr.DateFrom, fr.IsActive, fp.[Level]
    from
    (select fundID as ID from tblFund) f
    cross apply fnFundParents(f.ID) fp
    join tblFundRecommendation fr on fr.FundID = fp.FundID
    join tblRecommendation r on fr.RecommendationID = r.RecommendationID
    where fr.isActive=1
    --order by f.ID, datefrom desc
    )
    select distinct isff.FundID, tt.parentFundID, isff.instrumentid, isff.apircode, isff.investmentFullName
    , isff.InvestmentStatus, isff.IsWholesale, isff.isMainTaxStructure, T.Recommendation, T.DateFrom as RecommendationDateFrom
    , T.isActive as RecommendationStatus
    from vewISF_Fund isff
    inner join {tt_name} tt on isff.FundID = tt.FundID
    inner join T on t.FundID = tt.parentFundID and t.luan_r=1
    order by tt.parentFundID, isff.FundID
    '''.format(tt_name=tt_name)



def double_check(db, output):
    report_id = 11
    tt_name = TempTable.create_from_query(db, qry_get_fund_hierarchy())

    data = db.get_data(qry_get_investment_detail(tt_name))

    csvwriter = csv.writer(output, lineterminator='\n')
    header = ['FundID', 'ParentFundID', 'Instrumentid', 'Apircode', 'InvestmentFullName', 'InvestmentStatus'
              , 'IsWholesale', 'isMainTaxStructure', 'Recommendation', 'RecommendationDateFrom', 'RecommendationStatus']
    csvwriter.writerow(header)

    def cleanup(row):
        if row[0] == row[1]:
            row[1] = ''
        return row

    data = [cleanup(row) for row in data]
    csvwriter.writerows(data)


def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('-o', '--output', type=argparse.FileType('w'))
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
