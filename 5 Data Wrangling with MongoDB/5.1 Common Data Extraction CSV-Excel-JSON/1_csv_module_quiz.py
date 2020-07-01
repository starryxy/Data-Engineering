#!/usr/bin/env python
"""
Process the supplied file and use the csv module to extract data from it.
The data comes from NREL (National Renewable Energy Laboratory) website. Each file
contains information from one meteorological station, in particular - about amount of
solar and wind energy for each hour of day.

Note that the first line of the datafile is neither data entry, nor header. It is a line
describing the data source. Extract the name of the station from it.

The data should be returned as a list of lists (not dictionaries).
Use the csv modules "reader" method to get data in such format.
Use next() method to get the next line from the iterator.
"""
import csv
import os

DATADIR = ""
DATAFILE = "745090.csv"

# https://docs.python.org/2/library/csv.html#csv.reader
# https://docs.python.org/2/library/csv.html#reader-objects
def parse_file(datafile):
    name = ""
    data = []
    with open(datafile,'rb') as f:
        datareader = csv.reader(f)
        name = next(datareader)[1]
        header = next(datareader)
        for row in datareader:
            data.append(row)

    return (name, data)


def test():
    datafile = os.path.join(DATADIR, DATAFILE)
    name, data = parse_file(datafile)

    assert name == "MOUNTAIN VIEW MOFFETT FLD NAS"
    assert data[0][1] == "01:00"
    assert data[2][0] == "01/01/2005"
    assert data[2][5] == "2"


if __name__ == "__main__":
    test()
