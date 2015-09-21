__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
from mssqlwrapper import DB, TempTable
import logging
import helper

logger = logging.getLogger(__name__)


@helper.debug
def qry_is_lonsec_user(client_id):
    return '''
    select count(*) from LonsecLogin..fnClientAncestors({client_id}) where ClientID = 153
    '''.format(client_id=client_id)

@helper.debug
def qry_get_client_detail(client_id):
    return '''
    select ClientNameCombined, Email, Tel
    from fnClientDetails({client_id}, null)
    '''.format(client_id=client_id)


def is_lonsec_user(db, client_id):
    count = db.get_one_value(qry_is_lonsec_user(client_id))
    return True if count > 0 else False


def get_client_detail(db, client_id):
    data = db.get_data(qry_get_client_detail(client_id))
    return [(x.ClientNameCombined, x.Email, x.Tel) for x in data]


def add(db, client_id, domain_login, lonsec_contact_id):

    if not is_lonsec_user:
        raise Exception('{} is not Lonsec'.format(client_id))

    client_detail = get_client_detail(db, client_id)[0]

    data_dict = dict(name=client_detail[0], email=client_detail[1], tel=client_detail[2], domain_login=domain_login
                     , is_active=1)
    data_dict = ['@{k}={v!r}'.format(k=k.replace('_', ''), v=v) for k, v in data_dict.items() if v]

    proc_query = '''
    exec prcLonsecContactPut {params}
    '''.format(params=','.join(data_dict))
    logger.info(proc_query)

    rows = db.get_data(proc_query)

    raise_error = False
    try:
        next(db)
        raise_error = True   # should not have another set of data
    except:
        logger.info('LonsecContactID: {}'.format(rows[0][0]))

    if raise_error:
        raise Exception('Should not need to create new sector, please check your sector again')

def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('--server', default=r'MEL-TST-001\WEBSQL', help='Database Server')
    parser.add_argument('--database', default=r'Lonsec', help='Database Name')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('--client-id', help='Lonsec Client ID', type=int, required=True)
    parser.add_argument('--lonsec-contact-id', help='Lonsec Client ID', type=int)
    parser.add_argument('--domain-login', help='Domain Login MELBOURNE\\xxx', required=True)

    parser.add_argument('--dry-run', help='An excel file (normally from Jen Lee)', action='store_true')

    a = parser.parse_args()

    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)

    connection_string1 = r'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};' \
                         'Trusted_Connection=yes;'.format(server=a.server, database=a.database)

    db = DB.from_connection_string(connection_string1)
    if a.verbose > 1:
        db.debug = True

    add(db, a.client_id, a.domain_login, a.lonsec_contact_id)

    if not a.dry_run:
        logger.info('Commit changes')
        db.commit()
    else:
        logger.info('All changes did not commit')

if __name__ == '__main__':
    consoleUI()
