__author__ = 'Lmai'
import datetime
from reader import ExcelReader
from mssqlwrapper import TempTable
import logging
import pyodbc


logger = logging.getLogger(__name__)


def debug(fn):

    def wraps(*args, **kwargs):
        query = fn(*args, **kwargs)
        logger.info('\n{}:\n{}'.format(fn.__name__, query))
        return query

    return wraps


def upload_excel_to_tempdb(db, excel_file, sheet_name_or_idx):
    def get_data_from_excel():
        reader = ExcelReader(excel_file)
        rows = reader.get_data_from_sheet(sheet_name_or_idx)
        return rows, reader.create_qry
    tt_name = TempTable.create_from_data(db, *get_data_from_excel())

    try:
        # Extract records in [ExternalData].dbo.tblGrowthSeries that matches InvestmentID in temp table imported above
        date_value = db.get_one_value('select top 1 [date] from {}'.format(tt_name))

        if not isinstance(date_value, datetime.datetime):
            if isinstance(date_value, str):
                logger.info('Convert varchar to datetime for [date] column')

                # update table to yyyy-mm-dd format before convert to datetime type
                db.execute('''
                    update {}
                    set [date]=right([date],4)+'-'+SUBSTRING([date], 4, 2) + '-' + left([date],2)
                '''.format(tt_name))
            elif isinstance(date_value, float):
                logger.info('Convert float to datetime for [date] column')
                # SQL Server counts its dates from 01/01/1900 and Excel from 12/30/1899 = 2 days less.
                # update table to yyyy-mm-dd format before convert to datetime type
                db.execute('''
                    alter table {tt_name}
                    alter column [date] varchar(20)
                '''.format(tt_name=tt_name))

                db.execute('''
                    update {tt_name}
                    set date=cast(date - 2 as datetime)
                '''.format(tt_name=tt_name))

            db.execute('''
                alter table {}
                alter column [date] date
            '''.format(tt_name))
    except pyodbc.ProgrammingError:
        logger.info("{tt_name} does not have date column".format(tt_name=tt_name))

    return tt_name


def backup_table(db, tt_name):
    logger.info('Backing up table {}'.format(tt_name))

    today = datetime.date.today()
    today = today.strftime('%Y%m%d')
    new_tt_name = '{tt_name}_{date}'.format(tt_name=tt_name, date=today)

    if not db.sp_columns(new_tt_name):
        # if table does not exist
        logger.info('Creating {new_tt_name}'.format(new_tt_name=new_tt_name))
        db.execute('select * into {new_tt_name} from {tt_name}'.format(tt_name=tt_name, new_tt_name=new_tt_name))
    else:
        logger.info('Table {new_tt_name} already exists'.format(new_tt_name=new_tt_name))


def get_financial_year():
    today = datetime.date.today()
    return today.year + (1 if today.month >= 7 else 0)
