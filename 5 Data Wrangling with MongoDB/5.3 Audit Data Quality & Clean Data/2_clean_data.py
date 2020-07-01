"""
Your task is to check the "productionStartYear" of the DBPedia autos datafile for valid values.

The following things should be done:
- check if the field "productionStartYear" contains a year
- check if the year is in range 1886-2014
- convert the value of the field to be just a year (not full datetime)
- the rest of the fields and values should stay the same
- if the value of the field is a valid year in the range as described above,
  write that line to the output_good file
- if the value of the field is not a valid year as described above,
  write that line to the output_bad file
- discard rows (neither write to good nor bad) if the URI is not from dbpedia.org
- you should use the provided way of reading and writing data (DictReader and DictWriter)
  They will take care of dealing with the header.
"""
# https://developers.google.com/edu/python/regular-expressions
# https://www.rexegg.com/regex-quickstart.html

import csv
import pprint
import re

INPUT_FILE = 'autos.csv'
OUTPUT_GOOD = 'autos-valid.csv'
OUTPUT_BAD = 'FIXME-autos.csv'

def process_file(input_file, output_good, output_bad):
    good = []
    bad = []
    with open(input_file, "r") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        next(reader)
        for line in reader:
            uri_re = re.search(r'dbpedia\.org', line["URI"])
            print(line["URI"])
            if uri_re is not None:
                year_re = re.search(r'\d\d\d\d', line["productionStartYear"])
                print(line["productionStartYear"])
                if year_re is not None:
                    print(year_re.group())
                    if int(year_re.group()) >= 1886 and int(year_re.group()) <= 2014:
                        line["productionStartYear"] = year_re.group()
                        good.append(line)
                        print('good')
                    else:
                        bad.append(line)
                        print('bad')
                else:
                    bad.append(line)
                    print('bad')

    with open(output_good, "w") as g:
        writer = csv.DictWriter(g, delimiter=",", fieldnames = header)
        writer.writeheader()
        for row in good:
            writer.writerow(row)

    with open(output_bad, "w") as g:
        writer = csv.DictWriter(g, delimiter=",", fieldnames = header)
        writer.writeheader()
        for row in bad:
            writer.writerow(row)

    return output_good, output_bad

def test():

    process_file(INPUT_FILE, OUTPUT_GOOD, OUTPUT_BAD)

    f = open(OUTPUT_GOOD, "r")
    print("OUTPUT_GOOD")
    print(f.read())

    f = open(OUTPUT_BAD, "r")
    print("OUTPUT_BAD")
    print(f.read())


if __name__ == "__main__":
    test()
