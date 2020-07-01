import xlrd

datafile = "2013_ERCOT_Hourly_Load_Data.xls"


def parse_file(datafile):
    workbook = xlrd.open_workbook(datafile)
    sheet = workbook.sheet_by_index(0)

    data = [[sheet.cell_value(r, col)
                for col in range(sheet.ncols)]
                    for r in range(sheet.nrows)]

    print "\nList Comprehension"
    print "data[3][2]:",
    print data[3][2]

    print "\nCells in a nested loop:"
    for row in range(sheet.nrows):
        for col in range(sheet.ncols):
            if row == 50:
                print sheet.cell_value(row, col),


    ### other useful methods:
    print "\nROWS, COLUMNS, and CELLS:"
    print "Number of rows in the sheet:",
    print sheet.nrows
    print "Value in cell (row 3, col 2):",
    print sheet.cell_value(3, 2)
    print "Type of data in cell (row 3, col 2):",
    print sheet.cell_type(3, 2)
    print "Get a slice of values in column 3, from rows 1-3:"
    print sheet.col_values(3, start_rowx=1, end_rowx=4)

    print "\nDATES:"
    print "Value in cell (row 1, col 0):",
    print sheet.cell_value(1, 0)
    print "Type of data in cell (row 1, col 0):",
    print sheet.cell_type(1, 0)
    exceldate = sheet.cell_value(1, 0)
    print "Date in Excel format:",
    print exceldate
    print "Convert date to a Python datetime tuple, from the Excel float:",
    print xlrd.xldate_as_tuple(exceldate, 0)

    return data

data = parse_file(datafile)


# find min, max, avg of COAST column, and the date associated with min/max
import xlrd
from zipfile import ZipFile
import pprint

datafile = "2013_ERCOT_Hourly_Load_Data.xls"


def open_zip(datafile):
    with ZipFile('{0}.zip'.format(datafile), 'r') as myzip:
        myzip.extractall()


def parse_file(datafile):
    workbook = xlrd.open_workbook(datafile)
    sheet = workbook.sheet_by_index(0)

    coast_data = sheet.col_values(1, start_rowx=1, end_rowx=None)

    data = {}

    maxval = max(coast_data)
    minval = min(coast_data)

    max_index = coast_data.index(maxval) + 1
    min_index = coast_data.index(minval) + 1

    max_time = sheet.cell_value(max_index, 0)
    min_time = sheet.cell_value(min_index, 0)

    data['maxtime'] = xlrd.xldate_as_tuple(max_time, 0)
    data['maxvalue'] = round(maxval, 10)
    data['mintime'] = xlrd.xldate_as_tuple(min_time, 0)
    data['minvalue'] = round(minval, 10)
    data['avgcoast'] = round(sum(coast_data)/float(len(coast_data)), 10)

    return data

# xlrd.xldate_as_tuple(sometime, 0) can convert Excel date to Python tuple
# (year, month, day, hour, minute, second)

def test():
    open_zip(datafile)
    data = parse_file(datafile)

    assert data['maxtime'] == (2013, 8, 13, 17, 0, 0)
    assert round(data['maxvalue'], 10) == round(18779.02551, 10)


test()

data = parse_file(datafile)
pprint.pprint(data)

# https://academy.vertabelo.com/blog/python-array-vs-list/
