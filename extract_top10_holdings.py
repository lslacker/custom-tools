__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
import helper
import csv

logger = logging.getLogger(__name__)


def qry_get_apircode_from_citicode(tt_name):
    return '''
    select fp.FundProductCode, fv.FundVariationID as investmentID
    from ExternalData..tblFundProduct fp
    inner join tblFundVariation fv on fp.APIRCode = fv.APIRCode
    inner join {tt_name} tt on tt.Citicode = fp.FundProductCode
    '''.format(tt_name=tt_name)

@helper.debug
def qry_get_top_holdings(tt_name, data_provider):
    return '''
    select tt.FundProductCode, h.HoldingCode, dbo.fnProperCase(h.HoldingName) as HoldingName, th.Weight, th.DateFrom
    from {tt_name} tt
    left join tblInvestmentCode ic on tt.investmentID = ic.investmentID and ic.DataProviderID = {data_provider}
    left join ExternalData.dbo.tblFundProduct fp on fp.FundProductCode = ic.InvestmentCode and ic.DataProviderID = fp.DataProviderID
    left join ExternalData.dbo.tblTopHoldings th on th.UnderlyingFundID = fp.UnderlyingFundID and th.DataProviderID = fp.DataProviderID
    left join ExternalData.dbo.tblHolding h on h.ID = th.HoldingID and h.DataProviderID = th.DataProviderID
    where getdate() between th.DateFrom and th.DateTo
    order by tt.FundProductCode, th.Weight desc
    '''.format(tt_name=tt_name, data_provider=data_provider)


def extract(db, excel_file, sheet_name_or_idx, data_provider, output):
    # Import excel file into temp table
    tt_name = helper.upload_excel_to_tempdb(db, excel_file, sheet_name_or_idx)
    another_tt_name = TempTable.create_from_query(db, qry_get_apircode_from_citicode(tt_name))
    orig_citi_codes = db.get_data('select citicode from {}'.format(tt_name))
    orig_citi_codes = {code.citicode for code in orig_citi_codes}
    logger.info(orig_citi_codes)

    assert len(tt_name) == len(another_tt_name)


    holding_tt_name = TempTable.create_from_query(db, qry_get_top_holdings(another_tt_name, data_provider))

    rows = db.get_data('select * from {}'.format(holding_tt_name))
    unique_citi_codes = {row[0] for row in rows}
    logger.info(orig_citi_codes-unique_citi_codes)

    csvwriter = csv.writer(output, lineterminator='\n')
    header = 'FundProductCode,HoldingCode,HoldingName,Weight,DateFrom'
    csvwriter.writerow(header.split(','))
    csvwriter.writerows(rows)


def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', default=r'MEL-TST-001\WEBSQL', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('-i', '--input', help='An excel file (normally from Jen Lee)', required=True)
    parser.add_argument('-o', '--output', help='CSV otuput file', type=argparse.FileType('w'), required=True)
    parser.add_argument('--sheet', help='Sheet Name or Sheet Index', required=True)
    parser.add_argument('--data-provider', help='ETF: 4, Lonsec: 1, more at tblDataProvider', default=3, required=True)
    parser.add_argument('--dry-run', action='store_true')

    a = parser.parse_args()

    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)

    connection_string = r'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};' \
                         'Trusted_Connection=yes;'.format(server=a.server, database=a.database)

    db = DB.from_connection_string(connection_string)

    if a.verbose > 1:
        db.debug = True

    extract(db, a.input, a.sheet, a.data_provider, a.output)

    if not a.dry_run:
        logger.info('Commit changes')
        db.commit()
    else:
        logger.info('All changes did not commit')

if __name__ == '__main__':
    consoleUI()
