__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
import helper
import urllib.request
import urllib.error
import requests

logger = logging.getLogger(__name__)

@helper.debug
def qry_get_existing_fund_update(table_name, report_id, tt_name=None):

    extra_pred = 'and sectorID not in (select sectorID from {tt_name})'.format(tt_name=tt_name) if tt_name else ''

    query = '''
    select fr.*
    from {table_name} fr
    cross apply fnFundDetails(fr.fundID, default, default, default) fd
    where fr.reportid={report_id} and fr.isActive=1 {extra_pred}
    '''.format(report_id=report_id, table_name=table_name, extra_pred=extra_pred)

    return query

@helper.debug
def qry_get_existing_investment_viewpoints(table_name, report_id):
    query = '''
    select ir.InvestmentID, ve.StockCode
    from {table_name} ir
    left join vewEquities ve on ir.InvestmentID = ve.StockID
    where reportID={report_id} and isActive=1
    '''.format(report_id=report_id, table_name=table_name)

    return query


@helper.debug
def qry_get_existing_fund_profile(table_name, report_id):
    query = '''
    select  ir.InvestmentID, CASE
        WHEN isNULL(fv.APIRCode, '') <> '' then fv.APIRCode
        WHEN (i.InvestmentTypeID = 1) AND (sc.SectorID IS NULL) THEN 'MF-' + convert(varchar,i.InvestmentTypeID) + '-' + convert(varchar,i.InvestmentID)
        WHEN (i.InvestmentTypeID = 1) AND (sc.SectorID IS NOT NULL) THEN 'SP-' + convert(varchar,i.InvestmentTypeID) + '-' + convert(varchar,i.InvestmentID)
    END as StockCode
    from {table_name} ir
    left join tblFundVariation fv on ir.investmentid = fv.FundVariationID
    left join tblFund f on fv.fundID = f.fundID
    left join (select SectorID from fnSectorChildren(243, 1)) sc on f.sectorID = sc.SectorID
    left join tblInvestment i on fv.FundVariationID = i.InvestmentID
    where ir.reportID={report_id} and ir.isActive=1
    '''.format(report_id=report_id, table_name=table_name)

    return query


@helper.debug
def qry_get_direct_property_sector():
    return '''
    select * from fnSectorChildren(420, 1)
    union
    select * from fnSectorChildren(366, 1)
    union
    select * from fnSectorChildren(130, 1)
    union
    select * from fnSectorChildren(535, 1)
    union
    select * from fnSectorChildren(243, 1)
    '''

@helper.debug
def qry_deactive_old_report(report_id, fund_ids):
    return '''
    update tblFundReport
    set isActive=0
    where reportid={report_id} and isActive=1 and fundid in ({fund_ids})
    '''.format(report_id=report_id, fund_ids=','.join(map(str, fund_ids)))

@helper.debug
def qry_deactive_invalid_investment_report(report_id, investment_ids):
    return '''
    update tblInvestmentReport
    set isActive=0
    where reportid={report_id} and isActive=1 and investmentid in ({investment_ids})
    '''.format(report_id=report_id, investment_ids=','.join(map(str, investment_ids)))

@helper.debug
def qry_update_investment_viewpoint(investment_id, stock_code, report_id):
    return '''
    update tblInvestmentReport
    set reportURL='{stock_code}', regenerate=0
    where investmentid = {investment_id} and isActive=1 and reportid = {report_id}
    '''.format(stock_code=stock_code, investment_id=investment_id, report_id=report_id)


def get_parent_id(db, fund_id):
    parent_fund_id = db.get_one_value('''
    select fundid from fnFundParents({fund_id})
    where level=1
    '''.format(fund_id=fund_id))

    fund_id = get_parent_id(db, parent_fund_id) if parent_fund_id else fund_id

    return fund_id


def get_report_url(db, fund_id):
    reportURLs = db.get_data('''
    select isNull(APIRCode, InstrumentID)
    from vewISF_Fund
    where fundid=? and isMainTaxStructure=1
    ''', fund_id)

    if len(reportURLs) > 1:
        raise Exception('{} has more than 1 main tax structure'.format(fund_id))

    if len(reportURLs) < 1:
        return None

    return reportURLs[0][0]


def add_report(db, fund_id, report_id, report_url, analyst_id, authoriser_id):
    count = db.execute('''
        prcFundReportPut @fundid={fund_id}, @reportid={report_id}, @reportURL='{report_url}'
                        ,@IsActive=1,@AnalystID={analyst_id}, @AuthoriserID={authoriser_id}
    '''.format(fund_id=fund_id, report_id=report_id, report_url=report_url, analyst_id=analyst_id, authoriser_id=authoriser_id))

    return count


# def link_exists(urllink):
#     try:
#         with urllib.request.urlopen(urllink) as response:
#             return 'application/pdf' == response.info()['content-type']
#     except urllib.error.HTTPError:
#         return False

def link_exists(urllink):
    try:
        r = requests.get(urllink, stream=True)
        return True if r.headers['content-type'] == 'application/pdf' else False
    except:
        return False

def does_fund_has_recom(db, fund_id):
    data = db.get_data('''
    select fr.*
    from tblFundRecommendation fr
    inner join tblRecommendation r on fr.RecommendationID = r.RecommendationID
    where r.Recommendation not like '%Screen%' and fr.FundID=?
    ''', fund_id)

    return True if data else False


def add(db, from_report_id, to_report_id, report_type):
    table_name = 'tbl{report_type}'.format(report_type=report_type)
    helper.backup_table(db, table_name)
    if from_report_id != to_report_id:
        # from report 1 to report 35
        # update report 1 to
        direct_property_sector_tt_name = TempTable.create_from_query(db, qry_get_direct_property_sector())
        report_tt_name = TempTable.create_from_query(db, qry_get_existing_fund_update(table_name, from_report_id,
                                                                                  direct_property_sector_tt_name))

        data = db.get_data('''
        select tt.*, isnull(f.APIRCode, f.instrumentid) as newReportURL
        from {tt_name} tt
        left join vewISF_Fund f on tt.fundid = f.fundid and f.IsMainTaxStructure=1
        '''.format(tt_name=report_tt_name))

        assert len(report_tt_name) == len(data)
        ok = []
        errors = []
        no_recommd = []
        #data = data[:1]
        for row in data:
            fund_id = row.FundID
            parent_fund_id = get_parent_id(db, fund_id)
            report_url = get_report_url(db, parent_fund_id)
            logger.info('{} has parent ID of {}'.format(fund_id, parent_fund_id))
            if report_url is None:
                logger.info('{} {} has less than 1 main tax structure'.format('-'*80, parent_fund_id))
                errors += [fund_id]
            else:
                urllink = 'https://reports.lonsec.com.au/FV/{}'.format(report_url)
                logger.info('Checking {} exists'.format(urllink))
                is_link_exists = link_exists(urllink) if fund_id in [9356,8416,8432,14703,12883,548,15933,16086] else True
                if is_link_exists:
                    logger.info('----> OK')
                    ok += [fund_id]
                    #Lonsec..prcFundReportPut @fundid=4912, @reportid=33, @reportURL='YOC0100AU',@IsActive=1
                    # ,@AnalystID=56526, @AuthoriserID=56036
                    count = add_report(db, fund_id, to_report_id, report_url, analyst_id=56526, authoriser_id=56036)
                    logger.info('{} inserted'.format(count))
                elif not does_fund_has_recom(db, fund_id):
                    no_recommd += [fund_id]
                else:
                    logger.info('----> REPORT MISSING')
                    errors += [fund_id]

        logger.info('GOOD: {}'.format(len(ok)))
        logger.info('ERROR: {}'.format(len(errors)))
        logger.info('---> {}'.format(','.join(map(str, errors))))
        logger.info('NO RECOMENDATIONS OR SCREENOUT: {}'.format(len(no_recommd)))
        logger.info('---> {}'.format(','.join(map(str, no_recommd))))

        assert len(ok) + len(errors) + len(no_recommd) == len(data)

        # mark all of fund_id from from_report_id to deactive
        all_fund_ids = ok + errors + no_recommd

        count = db.execute(qry_deactive_old_report(from_report_id, all_fund_ids))

        logger.info('{} is marked inactive'.format(count))

        # due to trigger, count may not return correct value
        # ignore for now
        #assert count == len(data)

    elif from_report_id == 24:   # etf view point

        # investmentreport, just change to stock code
        data = db.get_data(qry_get_existing_investment_viewpoints(table_name, from_report_id))
        data = [(row.InvestmentID, row.StockCode) for row in data]
        errors = []
        ok = []
        no_reports = []
        for investment_id, stock_code in data:
            if stock_code:
                urllink = 'https://reports.lonsec.com.au/FV/{}'.format(stock_code)
                logger.info('Checking {} exists'.format(urllink))

                is_link_exists = link_exists(urllink)
                if is_link_exists:
                    logger.info('----> OK')
                    count = db.execute(qry_update_investment_viewpoint(investment_id, stock_code, to_report_id))
                    logger.info('{} updated'.format(count))
                    ok += [investment_id]
                else:
                    logger.info('----> REPORT MISSING')
                    no_reports += [investment_id]
            else:
                errors += [investment_id]

        logger.info('GOOD: {}'.format(len(ok)))
        logger.info('NO REPORTS: {}'.format(len(no_reports)))
        logger.info('---> {}'.format(','.join(map(str, no_reports))))
        logger.info('ERROR: {} (No stock codes)'.format(len(errors)))
        logger.info('---> {}'.format(','.join(map(str, errors))))

        assert len(ok) + len(errors) + len(no_reports) == len(data)

        # mark all of fund_id from from_report_id to deactive
        all_investment_ids = errors + no_reports

        count = db.execute(qry_deactive_invalid_investment_report(from_report_id, all_investment_ids))
        logger.info('{} is marked inactive'.format(count))

    elif from_report_id == 11:  # fund profile
        data = db.get_data(qry_get_existing_fund_profile(table_name, from_report_id))
        data = [(row.InvestmentID, row.StockCode) for row in data]
        errors = []
        ok = []
        no_reports = []
        for investment_id, stock_code in data:
            if stock_code:
                urllink = 'https://reports.lonsec.com.au/FP/{}'.format(stock_code)
                logger.info('Checking {} exists'.format(urllink))
                #is_link_exists = link_exists(urllink)
                is_link_exists = True
                if is_link_exists:
                    logger.info('----> OK')
                    count = db.execute(qry_update_investment_viewpoint(investment_id, stock_code, to_report_id))
                    logger.info('{} updated'.format(count))
                    ok += [investment_id]
                else:
                    logger.info('----> REPORT MISSING')
                    no_reports += [investment_id]
            else:
                errors += [investment_id]

        logger.info('GOOD: {}'.format(len(ok)))
        logger.info('NO REPORTS: {}'.format(len(no_reports)))
        logger.info('---> {}'.format(','.join(map(str, no_reports))))
        logger.info('ERROR: {} (No stock codes)'.format(len(errors)))
        logger.info('---> {}'.format(','.join(map(str, errors))))

        assert len(ok) + len(errors) + len(no_reports) == len(data)

        # mark all of fund_id from from_report_id to deactive
        all_investment_ids = errors + no_reports
        if all_investment_ids:
            count = db.execute(qry_deactive_invalid_investment_report(from_report_id, all_investment_ids))
            logger.info('{} is marked inactive'.format(count))
    else:
        raise Exception('not implemented')


def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('--report-type', help='Fund or Stock'
                        , choices=['fundreport', 'investmentreport']
                        , required=True)
    parser.add_argument('--from-report-id', help='From report id', type=int)
    parser.add_argument('--to-report-id', help='To report id', type=int)
    parser.add_argument('--dry-run', help='An excel file (normally from Jen Lee)', action='store_true')

    a = parser.parse_args()

    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)

    connection_string1 = r'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};' \
                         'Trusted_Connection=yes;'.format(server=a.server, database=a.database)

    db = DB.from_connection_string(connection_string1)
    if a.verbose > 1:
        db.debug = True

    add(db, a.from_report_id, a.to_report_id, a.report_type)

    if not a.dry_run:
        logger.info('Commit changes')
        db.commit()
    else:
        logger.info('All changes did not commit')

if __name__ == '__main__':
    consoleUI()
