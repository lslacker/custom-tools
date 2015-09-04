__author__ = 'Lmai'
from mssqlwrapper import DB, TempTable
import logging
from itertools import repeat, chain
import csv
from collections import defaultdict

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
    where ct.IsIndividual={is_individual} and cd.ClientTypeID<>3 and a.password <> '' and isnull(cd.email,'') <> ''
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


def qry_get_broker_advisor_codes(clientids):
    return '''
    select clientid, brokeradvisorcode
    from tblClient
    where clientid in ({clientids}) and isNull(brokeradvisorcode, '') <> ''
    '''.format(clientids=','.join(map(str, clientids)))


def translate_moduleid(db, clientmoduleid):
    logging.info('client module id: {}'.format(clientmoduleid))
    data = db.get_one_value(gry_get_module_name(clientmoduleid))
    return data



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

if __name__ == '__main__':
    connection_string1 = r'Driver={SQL Server Native Client 11.0};Server=mel-sql-002\WEBSQL;Database=LonsecLogin;' \
                         'Trusted_Connection=yes;'
    irate_connection_string = r'Driver={SQL Server Native Client 11.0};Server=mel-sql-005;Database=iRate;' \
                         'Trusted_Connection=yes;'
    octopus_connection_string = r'Driver={SQL Server Native Client 11.0};Server=mel-sql-001;Database=BackOffice;' \
                         'Trusted_Connection=yes;'
    db = DB.from_connection_string(connection_string1)
    irate_db = DB.from_connection_string(irate_connection_string)
    octopus_db = DB.from_connection_string(octopus_connection_string)

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
    #          'HasLicence,Comment,ClientSourceID,DateCreated,CreatedBy,DateUpdated,UpdatedBy,Password'
    #
    # with open(r'C:\Users\Lmai\Documents\Workspaces\ir\ir-1066_ldm-ioof-user-migration\test.csv', 'w') as f:
    #     csvwriter = csv.writer(f, lineterminator='\n')
    #     csvwriter.writerow(header.split(','))
    #     csvwriter.writerows(rows)

    clientids = [row[0] for row in rows]


    # get client advisor codes
    advisor_codes = db.get_data(qry_get_broker_advisor_codes(clientids))
    advisor_codes = [list(zip(repeat(code[0]), code[1].split(','))) for code in advisor_codes]
    print(advisor_codes)
    advisor_codes = list(chain(*advisor_codes))

    create_qry = '''
    create table {table_name} (
        clientid int,
        advisercode varchar(10) collate Latin1_General_CI_AS
    )
    '''
    tt_name = TempTable.create_from_data(octopus_db, advisor_codes, create_qry)

    rows = octopus_db.get_data(qry_get_advcode(tt_name))

    # orig_clientids = {x for x in clientids}
    # out_clientids = {row[0] for row in rows}
    # print("Below users do not have advcode")
    # print(orig_clientids - out_clientids)
    # with open(r'C:\Users\Lmai\Documents\Workspaces\ir\ir-1066_ldm-ioof-user-migration\test_clientadvcodes.csv', 'w') as f:
    #     csvwriter = csv.writer(f, lineterminator='\n')
    #     csvwriter.writerow(['userid', 'advcode'])
    #     csvwriter.writerows(rows)


    clientmoduleids_lookup = defaultdict(list)
    clientids = [27113]
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

    with open(r'C:\Users\Lmai\Documents\Workspaces\ir\ir-1066_ldm-ioof-user-migration\greenwood_mark.csv', 'w') as f:
        csvwriter = csv.writer(f, lineterminator='\n')
        csvwriter.writerow(['userid', 'groupname', 'productcode'])
        csvwriter.writerows(rows)
