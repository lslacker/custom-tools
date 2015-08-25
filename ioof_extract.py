__author__ = 'Lmai'
from mssqlwrapper import DB, TempTable
import logging
import csv
from collections import defaultdict
from itertools import chain


def qry_get_children(client_id):
    return '''
        select * from fnClientChildren({client_id}, 0)
        where isActive=1
    '''.format(client_id=client_id)


def qry_get_client(client_id, is_individual=True):
    return '''
    select cd.*, a.password from fnClientDetails({client_id}, default) cd
    inner join tblClientType ct on cd.clientTypeID = ct.ClientTypeID
    left join tblAuthentication a on cd.clientID = a.clientID
    where ct.IsIndividual={is_individual}
    '''.format(client_id=client_id, is_individual=1 if is_individual else 0)

def qry_get_client_portfolio(client_id):
    return '''
    Lonsec.dbo.prcPortfolioGet @clientID={client_id}
    '''.format(client_id=client_id)


def gry_get_module_name(module_id):
    return '''
    select moduleName
    from tblmodule
    where moduleID={module_id}
    '''.format(module_id=module_id)


def qry_get_instrument_ids(portfolioids):
    return '''
    select distinct InstrumentID
    from Lonsec..tblPortfolioInvestment tpi
    inner join Lonsec..vewISF_Fund vf on tpi.InvestmentID = vf.InvestmentID
    where PortfolioID in ('{portfolioids}')
    '''.format(portfolioids="','".join(portfolioids))


def qry_irate_code(instrumentids):
    return '''
    select ProductName, ProductCode from tblproducts
    where INSTRUMENTID in ('{instrumentids}')
    '''.format(instrumentids="','".join(instrumentids))

def translate_moduleid(db, clientmoduleid):
    logging.info('client module id: {}'.format(clientmoduleid))
    data = db.get_one_value(gry_get_module_name(clientmoduleid))
    return data


if __name__ == '__main__':
    connection_string1 = r'Driver={SQL Server Native Client 11.0};Server=mel-tst-001\WEBSQL;Database=LonsecLogin;' \
                         'Trusted_Connection=yes;'
    irate_connection_string = r'Driver={SQL Server Native Client 11.0};Server=mel-sql-005;Database=iRate;' \
                         'Trusted_Connection=yes;'

    db = DB.from_connection_string(connection_string1)
    irate_db = DB.from_connection_string(irate_connection_string)

    logging.basicConfig(level=logging.DEBUG)

    client_id = 47475  # IOOF Group
    data = db.get_data(qry_get_children(client_id))

    clientids = [row[0] for row in data]

    rows = []
    for clientid in clientids:
        data = db.get_data(qry_get_client(clientid, is_individual=True))
        rows += data

    # header = 'ClientID,ParentID,Office,DealerGroup,ClientTypeID,ClientName,ClientFirstName,ClientNameCombined,' \
    #          'SalutationID,Salutation,ClientPositionID,Email,IsAddressInherited,Address,PostCodeID,PostCode,Locality,'\
    #          'State,Region,PostalAddress,PostalPostCodeID,PostalPostCode,PostalLocality,PostalState,PostalRegion,'\
    #          'IsTelInherited,Tel,Mobile,IsFaxInherited,Fax,BSB,BankAccountName,BankAccountNo,ABN,ClientCode,'\
    #          'BrokerAdvisorCode,LonsecContactID,AccountingCode,SendEmails,InviteToEvents,IsActive,StillExists,'\
    #          'HasLicence,Comment,ClientSourceID,DateCreated,CreatedBy,DateUpdated,UpdatedBy'
    #
    # with open(r'C:\Users\Lmai\Documents\Workspaces\ir\ir-1066_ldm-ioof-user-migration\test.csv', 'w') as f:
    #     csvwriter = csv.writer(f, lineterminator='\n')
    #     csvwriter.writerow(header.split(','))
    #     csvwriter.writerows(rows)

    clientids = [row[0] for row in rows]

    clientmoduleids_lookup = defaultdict(list)

    rows = []
    for clientid in clientids:
        #logging.info(clientid)
        portfolioids = db.get_data(qry_get_client_portfolio(clientid))
        portfolioids = [x.PortfolioID for x in portfolioids]
        instrumentids = db.get_data(qry_get_instrument_ids(portfolioids))
        instrumentids = [x[0] for x in instrumentids]
        iratecodes = irate_db.get_data(qry_irate_code(instrumentids))
        data = [[clientid, product_name, product_code] for product_name, product_code in iratecodes]
        rows += data

    with open(r'C:\Users\Lmai\Documents\Workspaces\ir\ir-1066_ldm-ioof-user-migration\test_productgroup.csv', 'w') as f:
        csvwriter = csv.writer(f, lineterminator='\n')
        csvwriter.writerow(['userid', 'groupname', 'productcode'])
        csvwriter.writerows(rows)
