__author__ = 'Lmai'
import xlrd
import datetime


class ExcelReader:

    def __init__(self, excel_file):
        self.workbook = xlrd.open_workbook(excel_file)
        self.header = None
        self.create_qry = 'CREATE TABLE  {table_name} (%s)'

    def show_sheets(self):
        return self.workbook.sheet_names()

    def get_data_from_sheet(self, sheet_name_or_idx, with_header=True):

        try:
            worksheet = self.workbook.sheet_by_index(sheet_name_or_idx)
        except TypeError:
            worksheet = self.workbook.sheet_by_name(sheet_name_or_idx)

        nrows = worksheet.nrows
        ncols = worksheet.ncols
        offset_rows = 0

        if with_header:
            self.header = worksheet.row_values(0)
            offset_rows += 1
        else:
            self.header = ['field{}'.format(i + 1) for i in range(ncols)]

        def get_type(cell):
            if cell.ctype == xlrd.XL_CELL_DATE:
                return 'DATETIME'
            elif cell.ctype == xlrd.XL_CELL_EMPTY or \
                            cell.ctype == xlrd.XL_CELL_BLANK or \
                            cell.ctype == xlrd.XL_CELL_TEXT:
                return 'VARCHAR(MAX) collate SQL_Latin1_General_CP1_CI_AS'
            elif cell.ctype == xlrd.XL_CELL_NUMBER:
                return 'FLOAT'
            elif cell.ctype == xlrd.XL_CELL_BOOLEAN:
                return 'BIT'
            else:
                raise Exception('Cell type is not ')

        # determine data type
        # [:-1] is to remove last comma
        self.create_qry %= '\n'.join(['[{}] {},'.format(field, get_type(worksheet.cell(offset_rows, colidx)))
                                      for colidx, field in enumerate(self.header)])[:-1]

        def get_value(cell):
            cell_value = cell.value
            if cell.ctype == xlrd.XL_CELL_DATE:
                cell_value = datetime.datetime(*xlrd.xldate_as_tuple(cell_value, self.workbook.datemode))
            return cell_value

        # real value
        return ([get_value(worksheet.cell(rowidx, colidx)) for colidx in range(ncols)]
                for rowidx in range(offset_rows, nrows))


