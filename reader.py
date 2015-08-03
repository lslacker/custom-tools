__author__ = 'Lmai'
import xlrd
import datetime

class ExcelReader:

    def __init__(self, excel_file):
        self.workbook = xlrd.open_workbook(excel_file)

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
            header = worksheet.row_values(0)
            yield header
            offset_rows += 1
        else:

        # determine data type

        # real value
        for rowidx in range(offset_rows, nrows):
            rows = []
            for colidx in range(ncols):
                cell = worksheet.cell(rowidx, colidx)
                cell_value = cell.value
                if cell.ctype == xlrd.XL_CELL_DATE:
                    cell_value = datetime.datetime(*xlrd.xldate_as_tuple(cell_value, self.workbook.datemode))
                rows += [cell_value]
            yield rows
        # cell = worksheet.cell(1, 3)
        #
        # print(dir(cell))
        # print(cell.ctype)
        # print(cell.xf_index)
        # print(worksheet.nrows)
        # print(worksheet.ncols)
        # print(cell)

        # for cell in worksheet.row_values(1):
        #     print(cell.xf_index)
        #     print(dir(cell))
        #     print(type(cell))


if __name__ == '__main__':
    fn = r'C:\Users\Lmai\My Temp\Growth series above threshold 03082015.xlsx'
    reader = ExcelReader(fn)
    rows = list(reader.get_data_from_sheet(0))
    print(rows)