__author__ = 'lslacker'
# -*- coding: utf-8 -*-
import argparse
import xlwt
import csv
import os
import logging


def merge(csvs, output):
    wb = xlwt.Workbook(encoding='latin1')

    for f in csvs:
        logging.info('Process {}'.format(f.name))
        csv_reader = csv.reader(f)
        ws = wb.add_sheet(os.path.basename(f.name))

        for row_idx, row in enumerate(csv_reader):
            for col_idx, cell in enumerate(row):
                ws.write(row_idx, col_idx, cell)
    wb.save(output)


def consoleUI():
    parser = argparse.ArgumentParser(description='Merge multiple csv files into excel file, each csv')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('-o', '--output', required=True)
    parser.add_argument('csvs', nargs='+', type=argparse.FileType('r', encoding='latin1'), help='CSV files')
    a = parser.parse_args()
    if a.verbose > 1:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

    merge(a.csvs, a.output)

if __name__ == '__main__':
    consoleUI()




