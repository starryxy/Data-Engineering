import os
import pprint
import csv
# https://docs.python.org/2/library/csv.html
# https://docs.python.org/2/library/pprint.html

DATADIR = ""
DATAFILE = "beatles-diskorgraphy.csv"

def parse_csv(datafile):
    data = []
    with open(datafile, 'rb') as sd:
        r = csv.DictReader(sd)
        for line in r:
            data.append(line)
    return data


if __name__ == '__main__':
    datafile = os.path.join(DATADIR, DATAFILE)
    parse_csv(datafile)
    d = parse_csv(datafile)
    pprint.pprint(d)


# without csv module, create a dict for first 10 lines
# skip header, split each line with ",", remove extra whitespaces before/after each field/value
import os

DATADIR = ""
DATAFILE = "beatles-diskography.csv"

def parse_file(datafile):
    data = []
    with open(datafile, "r") as f:
        # get keys for values, split keys using comma delimiter
        header = f.readline().split(",")
        n = 0
        for line in f:
            if n == 10:
                break

            fields = line.split(",")
            entry = {}

            # get index of each value in fields, and find the according header/key
            for i, value in enumerate(fields):
                # match key and value
                # strip() can remove extra whitespace
                entry[header[i].strip()] = value.strip()

            data.append(entry)
            n += 1

    return data


def test():
    datafile = os.path.join(DATADIR, DATAFILE)
    d = parse_file(datafile)
    firstline = {'Title': 'Please Please Me', 'UK Chart Position': '1', 'Label': 'Parlophone(UK)', 'Released': '22 March 1963', 'US Chart Position': '-', 'RIAA Certification': 'Platinum', 'BPI Certification': 'Gold'}
    tenthline = {'Title': '', 'UK Chart Position': '1', 'Label': 'Parlophone(UK)', 'Released': '10 July 1964', 'US Chart Position': '-', 'RIAA Certification': '', 'BPI Certification': 'Gold'}

    assert d[0] == firstline
    assert d[9] == tenthline


test()
