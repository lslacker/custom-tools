from functools import reduce

__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
import helper
import operator
import itertools

logger = logging.getLogger(__name__)


@helper.debug
def qry_get_modules(tt_name, modules):
    return '''
    select convert(int, tt.clientID) as inputClientID, cma.ClientID, cma.clientModuleID, cma.ModuleID
    from {tt_name} tt
    cross apply LonsecLogin..fnClientModuleAccess(default, tt.ClientID, 1, 0, getdate()) cma
    where cma.moduleID in ({modules}) and cma.hasAccess=1
    '''.format(tt_name=tt_name, modules=','.join(modules))

@helper.debug
def qry_update_expiry(clientid, moduleid):
    return '''
    update LonsecLogin..tblClientModule
    set ToDate = '2015-10-31 00:00:00'
    where moduleID = {moduleid} and clientid = {clientid} and ToDate = '2079-06-06 00:00:00'
    '''.format(moduleid=moduleid, clientid=clientid)

@helper.debug
def qry_update_deny_access(clientid, clientmoduleid):
    return '''
    exec LonsecLogin.dbo.prcClientModuleAccessPut @ClientModuleID={clientmoduleid}, @ClientID={clientid}
            , @HasExclusive=0, @IsDenied=1, @IsGranted=0, @ChildInherits=0
    '''.format(clientmoduleid=clientmoduleid, clientid=clientid)

def upload_benchmark(db, excel_file, sheet_name_or_idx):
    # Import excel file into temp table
    tt_name = helper.upload_excel_to_tempdb(db, excel_file, sheet_name_or_idx)

    modules = [4170, 4171, 4172, 4173]
    modules = map(str, modules)

    data = db.get_data(qry_get_modules(tt_name, modules))
    ok = [db.execute(qry_update_expiry(row[0], row[-1])) for row in data if row[0] == row[1]]
    # notok = [db.execute(qry_update_deny_access(row[0], row[-2])) for row in data if row[0] != row[1]]
    # assert reduce(operator.add, ok) + len(notok) == len(data)

def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', default=r'MEL-TST-001\WEBSQL', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('-i', '--input', help='An excel file (normally from Jen Lee)', required=True)
    parser.add_argument('--sheet', help='Sheet Name or Sheet Index', required=True)
    parser.add_argument('--dry-run', action='store_true')

    a = parser.parse_args()

    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)

    connection_string = r'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};' \
                         'Trusted_Connection=yes;'.format(server=a.server, database=a.database)

    db = DB.from_connection_string(connection_string)

    if a.verbose > 1:
        db.debug = True


    upload_benchmark(db, a.input, a.sheet)

    if not a.dry_run:
        logger.info('Commit changes')
        db.commit()
    else:
        logger.info('All changes did not commit')

if __name__ == '__main__':
    consoleUI()
