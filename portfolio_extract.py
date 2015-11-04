__author__ = 'Lmai'
from mssqlwrapper import DB
import logging
import helper


logger = logging.getLogger(__name__)


def extract_modelportfolio(db, output):

    # def qry_get_investment_name():
    #     return '''
    #     select InvestmentListID, InvestmentListName from tblInvestmentList
    #     where isActive=1 and parentID is null and
    #         (investmentListName like '%Aon%Model Portfolios'
    #          or investmentListName like '%ASET%Model Portfolio%'
    #          or investmentListName like '%Aylesbury%Portfolio%'
    #          or investmentListName like '%BFP%Phase%'
    #          or investmentListName like '%Camerons%Portfolios%'
    #          or investmentListName like '%FSS%Portfolios%'
    #          or investmentListName like '%BT Panorama%Portfolios%'
    #          or investmentListName like '%DAC objective%Portfolio%'
    #          or investmentListName like '%Lonsec Retirement%Portfolios%'
    #          or investmentListName like '%LFG Model Portfolios%'
    #          or investmentListName like '%AssetChoice Ess%Portfolios%'
    #     )
    #     '''

    def qry_get_investment_name():
        return '''
        select InvestmentListID, InvestmentListName from tblInvestmentList
        where isActive=1 and parentID is null and
            (investmentListName like 'UniSuper%Portfolio%'

        )
        '''

    def qry_get_investment_list_investment_weight_details(listid):
        return '''
        With T(InvestmentListID, InvestmentListName, RiskCategoryNo) as (
        select InvestmentListID, InvestmentListName, RiskCategoryNo from tblInvestmentList
        where investmentListid={listid}
        union all
        select il.InvestmentListID, il.InvestmentListName, il.RiskCategoryNo
        from tblInvestmentList il
        inner join T on il.parentID = T.investmentListID
        )
        select T.InvestmentListName, rc.RiskCategory as Strategy, coalesce(ve.stockCode, isff.ApirCode
        , ic.investmentCode) as SecurityCode
        , case
           when ve.Exchange is not null then ve.Exchange
           when isff.InstrumentID is not null then 'FND'
           else 'CASH'
          end
        , coalesce(ve.stockName, isff.InvestmentName, 'CASH') as SecurityName
        , ili.[Weight], ili.DateFrom, ili.InvestmentID
        from T
        left join tblRiskCategory rc on T.RiskCategoryNo = rc.RiskCategoryNo
        left join tblInvestmentListInvestment ili on ili.investmentListID = T.InvestmentListID
                                                 and ili.DateTo = '2079-06-06'
        left join vewEquities ve on ili.InvestmentID = ve.StockID
        left join vewISF_Fund isff on isff.InvestmentID = ili.InvestmentID
        left join tblInvestmentCode ic on ic.InvestmentID = ili.InvestmentID and ic.IsUsedForGrowthSeries=1
        order by T.investmentListID, T.RiskCategoryNo
        '''.format(listid=listid)

    def replace_with_parent(row, parent):
        row[0] = parent
        return row

    rows = db.get_data(qry_get_investment_name())

    header = ['Portfolio', 'Strategy', 'Security Code', 'Exchange', 'Security Name', 'Weight', 'Effective Date', 'InvestmentID']
    report_data = []
    for row in rows:
        listid, listname = row
        logger.info('Processing {} - {}'.format(listname, listid))
        data = db.get_data(qry_get_investment_list_investment_weight_details(listid))
        logger.info(data)
        data = [replace_with_parent(row, listname) for row in data[1:]]  # don't count first row
        report_data += data

    helper.write_data(output, report_data, header)


def extract_benchmarkportfolio(db, output):

    # def qry_get_investment_name():
    #     return '''
    #     select InvestmentListID, InvestmentListName from tblInvestmentList
    #     where isActive=1 and parentID is null and
    #         (investmentListName like '%Lonsec (Traditional) SAA Benchmark%')
    #     '''

    def qry_get_investment_name():
        return '''
        select InvestmentListID, InvestmentListName from tblInvestmentList
        where isActive=1 and parentID is null and
            (investmentListName like '%Lonsec (Traditional) SAA Benchmark%')
        '''


    def qry_get_investment_list_benchmark_weight_details(listid):
        return '''
        With T(InvestmentListID, InvestmentListName, RiskCategoryNo) as (
        select InvestmentListID, InvestmentListName, RiskCategoryNo from tblInvestmentList
        where investmentListid={listid}
        union all
        select il.InvestmentListID, il.InvestmentListName, il.RiskCategoryNo
        from tblInvestmentList il
        inner join T on il.parentID = T.investmentListID
        )
        select T.InvestmentListName, 'Risk Profile '+ convert(varchar, T.RiskCategoryNo) as Strategy
        , ve.AlternativeCode as SecurityCode
        , 'CASH' as Exchange
        , ve.BenchmarkName
        , ilb.[Weight], ilb.DateFrom
        from T
        left join tblInvestmentListBenchmark ilb on ilb.investmentListID = T.InvestmentListID and ilb.DateTo = '2079-06-06'
        left join tblBenchmark ve on ilb.BenchmarkID = ve.BenchmarkID
        order by T.investmentListID, T.RiskCategoryNo
        '''.format(listid=listid)

    def replace_with_parent(row, parent):
        row[0] = parent
        return row

    rows = db.get_data(qry_get_investment_name())

    header = ['Portfolio', 'Strategy', 'Security Code', 'Exchange', 'Security Name', 'Weight', 'Effective Date']
    report_data = []
    for row in rows:
        listid, listname = row
        logger.info('Processing {} - {}'.format(listname, listid))
        data = db.get_data(qry_get_investment_list_benchmark_weight_details(listid))

        data = [replace_with_parent(row, listname) for row in data if row.Strategy]  # don't count first row
        report_data += data

    helper.write_data(output, report_data, header)

if __name__ == '__main__':
    connection_string1 = r'Driver={SQL Server Native Client 11.0};Server=mel-sql-002\WEBSQL;Database=Lonsec;' \
                         'Trusted_Connection=yes;'
    db = DB.from_connection_string(connection_string1)
    logging.basicConfig(level=logging.DEBUG)
    # output = r'C:\Users\Lmai\Documents\Workspaces\ir\ir-1191_unisuper-to-irate\\model_portfolio_extract.csv'
    # extract_modelportfolio(db, output)

    output = r'C:\Users\Lmai\Documents\Workspaces\ir\ir-1177_ldm-pe-model-benchmark-extract\benchmark_portfolio_extract.csv'
    extract_benchmarkportfolio(db, output)
