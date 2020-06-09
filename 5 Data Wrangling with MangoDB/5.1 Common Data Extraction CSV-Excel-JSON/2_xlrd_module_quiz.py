# -*- coding: utf-8 -*-
'''
Find the time and value of max load for each of the regions:
COAST, EAST, FAR_WEST, NORTH, NORTH_C, SOUTHERN, SOUTH_C, WEST
Write the result out in a csv file, using pipe character | as the delimiter.

An example output can be seen in the "example.csv" file.
'''

import xlrd
import os
import csv
from zipfile import ZipFile

datafile = "2013_ERCOT_Hourly_Load_Data.xls"
outfile = "2013_Max_Loads.csv"


def open_zip(datafile):
    with ZipFile('{0}.zip'.format(datafile), 'r') as myzip:
        myzip.extractall()


def parse_file(datafile):
    workbook = xlrd.open_workbook(datafile)
    sheet = workbook.sheet_by_index(0)
    data = [None, None, None, None, None, None, None, None]
    region = ["COAST", "EAST", "FAR_WEST", "NORTH", "NORTH_C", "SOUTHERN", "SOUTH_C", "WEST"]

    for i in range(1, 9):
        raw_data = sheet.col_values(i, start_rowx=1, end_rowx=None)
        maxval = max(raw_data)
        max_index = raw_data.index(maxval) + 1
        max_time = sheet.cell_value(max_index, 0)
        xl_time = xlrd.xldate_as_tuple(max_time, 0)
        data[i - 1] = [region[i - 1], xl_time[0], xl_time[1], xl_time[2], xl_time[3], maxval]
        print(data[i - 1])

    return data


def save_file(data, filename):
    with open(filename, "wb") as outfile:
        writer = csv.writer(outfile, delimiter = "|")
        header = ["Station", "Year", "Month", "Day", "Hour", "Max Load"]
        writer.writerow(header)
        for row in data:
            writer.writerow(row)

    return filename


def parse_csv(outfile):
    outdata = []
    with open(outfile,'rb') as f:
        datareader = csv.reader(f)
        header = next(datareader)
        print(header)
        for row in datareader:
            outdata.append(row)

    return outdata


open_zip(datafile)
data = parse_file(datafile)
save_file(data, outfile)
outdata = parse_csv(outfile)
print(outdata)


def test():
    open_zip(datafile)
    data = parse_file(datafile)
    save_file(data, outfile)

    number_of_rows = 0
    stations = []

    ans = {'FAR_WEST': {'Max Load': '2281.2722140000024',
                        'Year': '2013',
                        'Month': '6',
                        'Day': '26',
                        'Hour': '17'}}
    correct_stations = ['COAST', 'EAST', 'FAR_WEST', 'NORTH',
                        'NORTH_C', 'SOUTHERN', 'SOUTH_C', 'WEST']
    fields = ['Year', 'Month', 'Day', 'Hour', 'Max Load']

    with open(outfile) as of:
        csvfile = csv.DictReader(of, delimiter="|")
        for line in csvfile:
            station = line['Station']
            if station == 'FAR_WEST':
                for field in fields:
                    # Check if 'Max Load' is within .1 of answer
                    if field == 'Max Load':
                        max_answer = round(float(ans[station][field]), 1)
                        max_line = round(float(line[field]), 1)
                        assert max_answer == max_line

                    # Otherwise check for equality
                    else:
                        assert ans[station][field] == line[field]

            number_of_rows += 1
            stations.append(station)

        # Output should be 8 lines not including header
        assert number_of_rows == 8

        # Check Station Names
        assert set(stations) == set(correct_stations)


if __name__ == "__main__":
    test()
