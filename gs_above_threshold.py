__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
from functools import partial
import helper



logger = logging.getLogger(__name__)

def qry_unique_investment_id(tt_name):
    return '''
    select count(distinct investmentID)
    from {tt_name}
    '''.format(tt_name=tt_name)

def qry_current_growth_series(tt_name):
    return '''
    select distinct ic.*
    from tblInvestmentCode ic
    inner join {tt_name} tt on ic.investmentID = tt.investmentID and ic.isUsedForGrowthSeries = 1
    '''.format(tt_name=tt_name)


@helper.debug
def qry_update_gs(toggle_gs, tt_name):
    opposite = 1 - toggle_gs
    return '''
    update ic
    set isUsedForGrowthSeries = {toggle_gs}
    from tblInvestmentCode ic
    inner join {tt_name} tt on ic.investmentCodeID = tt.investmentCodeID
    and ic.isUsedForGrowthSeries = {opposite}
    '''.format(tt_name=tt_name, toggle_gs=toggle_gs, opposite=opposite)


def process(db, excel_file, sheet_name_or_idx):

    helper.backup_table(db, 'tblInvestmentGrowthSeries')

    # Import excel file into temp table
    tt_name = helper.upload_excel_to_tempdb(db, excel_file, sheet_name_or_idx)
    unique_investmentids = db.get_one_value(qry_unique_investment_id(tt_name))
    logger.info(unique_investmentids)

    current_gs_tt_name = TempTable.create_from_query(db, qry_current_growth_series(tt_name))

    qry_turn_off_gs = partial(qry_update_gs, 0, current_gs_tt_name)
    qry_turn_on_gs = partial(qry_update_gs, 1, current_gs_tt_name)

    assert unique_investmentids == len(current_gs_tt_name)

    db.execute(qry_turn_off_gs())   # ignore count
    db.execute(qry_turn_on_gs())   # ignore count


def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', default=r'MEL-TST-001\WEBSQL', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('-i', '--input', help='An excel file (normally from Jen Lee)', required=True)
    parser.add_argument('--sheet', help='Sheet Name or Sheet Index', required=True)
    parser.add_argument('--dry-run', help='An excel file (normally from Jen Lee)', action='store_true')

    a = parser.parse_args()

    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)

    connection_string1 = r'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};' \
                         'Trusted_Connection=yes;'.format(server=a.server, database=a.database)

    db = DB.from_connection_string(connection_string1)
    if a.verbose > 1:
        db.debug = True

    process(db, a.input, a.sheet)

    if not a.dry_run:
        logger.info('Commit changes')
        db.commit()
    else:
        logger.info('All changes did not commit')

if __name__ == '__main__':
    consoleUI()
