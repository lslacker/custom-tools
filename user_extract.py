__author__ = 'Lmai'
from mssqlwrapper import DB, TempTable
import logging
from itertools import repeat, chain
import csv
from collections import defaultdict
import helper
import functools

@helper.debug
def qry_get_children(client_id):
    return '''
        select * from fnClientChildren({client_id}, 0)
        where isActive=1
    '''.format(client_id=client_id)

@helper.debug
def qry_get_client(client_id, exclude_financial_advisor, is_individual=True, ignore_password=False):
    return '''
    select cd.*, a.password, a.LastLoginTime, ct.Description from fnClientDetails({client_id}, default) cd
    inner join tblClientType ct on cd.clientTypeID = ct.ClientTypeID
    left join tblAuthentication a on cd.clientID = a.clientID
    where ct.IsIndividual={is_individual} {with_exclude_fa} {ignore_password}
    '''.format(client_id=client_id
               , is_individual="1 and isnull(cd.email,'') <> ''" if is_individual else 0
               , with_exclude_fa=' and cd.ClientTypeID<>3' if exclude_financial_advisor else ''
               , ignore_password="and a.password <> ''" if not ignore_password else '')

@helper.debug
def qry_get_client_portfolio(client_id):
    return '''
    Lonsec.dbo.prcPortfolioGet @clientID={client_id}
    '''.format(client_id=client_id)

@helper.debug
def gry_get_module_name(module_id):
    return '''
    select moduleName
    from tblmodule
    where moduleID={module_id}
    '''.format(module_id=module_id)

@helper.debug
def qry_get_instrument_ids(portfolioids):
    return '''
    select distinct InstrumentID
    from Lonsec..tblPortfolioInvestment tpi
    inner join Lonsec..vewISF_Fund vf on tpi.InvestmentID = vf.InvestmentID
    where PortfolioID in ('{portfolioids}')
    '''.format(portfolioids="','".join(portfolioids))

@helper.debug
def qry_irate_code(instrumentids):
    return '''
    select ProductName, ProductCode from tblproducts
    where INSTRUMENTID in ('{instrumentids}')
    '''.format(instrumentids="','".join(instrumentids))

@helper.debug
def qry_get_broker_advisor_codes(clientids):
    return '''
    select clientid, brokeradvisorcode
    from tblClient
    where clientid in ({clientids}) and isNull(brokeradvisorcode, '') <> ''
    '''.format(clientids=','.join(map(str, clientids)))

@helper.debug
def translate_moduleid(db, clientmoduleid):
    logging.info('client module id: {}'.format(clientmoduleid))
    data = db.get_one_value(gry_get_module_name(clientmoduleid))
    return data


@helper.debug
def qry_get_advcode(tt_name):
    return '''
    select distinct t.clientid,
    case
        when am.brokeradvisercode is not null then am.advcode
        when am1.pershingAdviserCode is not null then am1.advcode
        when am2.advcode is not null then am2.advcode
        when a.advcode is not null then a.advcode
        else t.advisercode + '<not valid>'
    end as advcode
    from {tt_name} t
    left join BackOffice..tblAdviserMap am on am.brokeradvisercode = t.advisercode
    left join BackOffice..tblAdviserMap am1 on am1.pershingAdviserCode = t.advisercode
    left join BackOffice..tblAdviserMap am2 on am2.advcode = t.advisercode
    left join Octopus.dbo.advisor a on a.advcode = t.advisercode
    '''.format(tt_name=tt_name)


def get_childrens(db, root_client_id):
    data = db.get_data(qry_get_children(root_client_id))
    return [row[0] for row in data]


def get_client(is_individual, db, clientids, exclude_financial_advisor, ignore_password):
    temp = [db.get_data(qry_get_client(clientid, exclude_financial_advisor, is_individual, ignore_password)) for clientid in clientids]
    return [x.pop() for x in temp if x]


get_users = functools.partial(get_client, True)
get_company = functools.partial(get_client, False)


@helper.debug
def qry_get_external_client_mapping(clientids):
    return '''
    with t as (
        SELECT Distinct cLocal.ClientID, ecm.ExternalClientCode, ecm.ExternalClientName, ecm.OutputDest
        , ecm.SourceClientID, cExtProvider.ClientCode as SourceClientCode, cExtProvider.ClientName as SourceClientName
        FROM tblExternalClientMapping ecm
        JOIN tblClient cExtProvider ON cExtProvider.ClientID = ecm.SourceClientID
        JOIN tblClient cLocal ON cLocal.ClientID = ecm.LocalClientID
    )
    select * from t
    where clientID in ({clientids})
    '''.format(clientids=','.join(map(str, clientids)))


def get_external_client_mappping(db, clientids):
    return db.get_data(qry_get_external_client_mapping(clientids))


def qry_get_client_moduleid(client_id):
    return '''
    select cma.ModuleID, m.ModuleName
    from fnClientModuleAccess(default, {client_id}, default, 0, getdate()) cma
    inner join tblmodule m on cma.moduleID = m.moduleID
    where cma.clientID<>-1 and cma.hasAccess=1 and m.isActive=1
    order by cma.moduleID
    '''.format(client_id=client_id)


def get_client_subscriptions(db, clientids):
    return list(chain(*[[[clientid] + list(row) for row in db.get_data(qry_get_client_moduleid(clientid))] for clientid in clientids]))


def write_data(output, data, header=None):
    with open(output, 'w') as f:
        csvwriter = csv.writer(f, lineterminator='\n')
        if header:
            csvwriter.writerow(header.split(','))
        csvwriter.writerows(data)

if __name__ == '__main__':
    connection_string1 = r'Driver={SQL Server Native Client 11.0};Server=mel-sql-002\WEBSQL;Database=LonsecLogin;' \
                         'Trusted_Connection=yes;'
    irate_connection_string = r'Driver={SQL Server Native Client 11.0};Server=mel-sql-006;Database=iRate;' \
                         'Trusted_Connection=yes;'
    octopus_connection_string = r'Driver={SQL Server Native Client 11.0};Server=mel-sql-001;Database=BackOffice;' \
                         'Trusted_Connection=yes;'
    db = DB.from_connection_string(connection_string1)
    irate_db = DB.from_connection_string(irate_connection_string)
    octopus_db = DB.from_connection_string(octopus_connection_string)

    logging.basicConfig(level=logging.DEBUG)

    # root_client_id = 47475  # IOOF Group
    # root_client_id = 13693  # RSM Bird Cameron
    #root_client_id = 43966  # Semaphore Private
    #root_client_id = 54528  # AMP
    root_client_id = 35014  # Unisuper

    clientids = get_childrens(db, root_client_id)

    exclude_financial_advisor = False

    company_users = get_company(db, clientids, exclude_financial_advisor, ignore_password=True)
    individual_users = get_users(db, clientids, exclude_financial_advisor, ignore_password=True)

    header = 'ClientID,ParentID,Office,DealerGroup,ClientTypeID,ClientName,ClientFirstName,ClientNameCombined,' \
             'SalutationID,Salutation,ClientPositionID,Email,IsAddressInherited,Address,PostCodeID,PostCode,Locality,'\
             'State,Region,PostalAddress,PostalPostCodeID,PostalPostCode,PostalLocality,PostalState,PostalRegion,'\
             'IsTelInherited,Tel,Mobile,IsFaxInherited,Fax,BSB,BankAccountName,BankAccountNo,ABN,ClientCode,'\
             'BrokerAdvisorCode,LonsecContactID,AccountingCode,SendEmails,InviteToEvents,IsActive,StillExists,'\
             'HasLicence,Comment,ClientSourceID,DateCreated,CreatedBy,DateUpdated,UpdatedBy,Password,LastLogin, Description'

    write_data(r'C:\Users\Lmai\Documents\Workspaces\company_20150917.csv', company_users, header)
    write_data(r'C:\Users\Lmai\Documents\Workspaces\users_20150917.csv', individual_users, header)

    clientids = [row[0] for row in individual_users]

    # EXTERNAL CLIENT MAPPING
    header = 'ClientID,ExternalClientCode,ExternalClientName,OutputDest,SourceClientID,SourceClientCode,SourceClientName'
    data = get_external_client_mappping(db, clientids)
    write_data(r'C:\Users\Lmai\Documents\Workspaces\users_external_20150917.csv', data, header)

    # MODULES
    data = get_client_subscriptions(db, clientids)
    header = 'ClientID,ModuleID,ModuleName'
    write_data(r'C:\Users\Lmai\Documents\Workspaces\users_subscription_20150917.csv', data, header)

    # get client advisor codes
    advisor_codes = db.get_data(qry_get_broker_advisor_codes(clientids))
    advisor_codes = [list(zip(repeat(code[0]), code[1].split(','))) for code in advisor_codes]
    print(advisor_codes)
    advisor_codes = list(chain(*advisor_codes))
    if advisor_codes:
        create_qry = '''
        create table {table_name} (
            clientid int,
            advisercode varchar(10) collate Latin1_General_CI_AS
        )
        '''
        tt_name = TempTable.create_from_data(octopus_db, advisor_codes, create_qry)

        rows = octopus_db.get_data(qry_get_advcode(tt_name))

        orig_clientids = {x for x in clientids}
        out_clientids = {row[0] for row in rows}
        print("Below users do not have advcode")
        print(orig_clientids - out_clientids)
        with open(r'C:\Users\Lmai\Documents\Workspaces\test_clientadvcodes.csv', 'w') as f:
            csvwriter = csv.writer(f, lineterminator='\n')
            csvwriter.writerow(['userid', 'advcode'])
            csvwriter.writerows(rows)

    clientmoduleids_lookup = defaultdict(list)

    rows = []
    for clientid in clientids:
        #logging.info(clientid)
        portfolioids = db.get_data(qry_get_client_portfolio(clientid))
        #portfolioids = [x.PortfolioID for x in portfolioids]
        count = 1
        for portfolio in portfolioids:
            portfolioid = portfolio.PortfolioID
            portfolioname = portfolio.PortfolioName
            if not portfolioname:
                portfolioname = 'Portfolio {}'.format(count)
                count += 1

            instrumentids = db.get_data(qry_get_instrument_ids([portfolioid]))
            instrumentids = [x[0] for x in instrumentids]
            iratecodes = irate_db.get_data(qry_irate_code(instrumentids))
            data = [[clientid, portfolioname, product_code] for product_name, product_code in iratecodes]
            rows += data

    with open(r'C:\Users\Lmai\Documents\Workspaces\greenwood_mark.csv', 'w') as f:
        csvwriter = csv.writer(f, lineterminator='\n')
        csvwriter.writerow(['userid', 'groupname', 'productcode'])
        csvwriter.writerows(rows)
