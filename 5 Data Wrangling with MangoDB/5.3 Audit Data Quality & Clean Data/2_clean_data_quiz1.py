#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
In this problem set you work with cities infobox data, audit it, come up with a
cleaning idea and then clean it up.

Since in the previous quiz you made a decision on which value to keep for the
"areaLand" field, you now know what has to be done.

Finish the function fix_area(). It will receive a string as an input, and it
has to return a float representing the value of the area or None.
You have to change the function fix_area. You can use extra functions if you
like, but changes to process_file will not be taken into account.
The rest of the code is just an example on how this function can be used.
"""
import codecs
import csv
import json
import pprint

CITIES = 'cities.csv'

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def fix_area(area):

    print(area)

    if is_number(area):
        area = float(area)
    elif area[0] == '{':
        area0 = area.split("|")[0][1:]
        area1 = area.split("|")[1][:-1]
        if area0[::-1].find('.') >= area1[::-1].find('.'):
            area = float(area0)
        else:
            area = float(area1)
    else:
        area = None

    print(area)
    return area



def process_file(filename):
    data = []

    with open(filename, "r") as f:
        reader = csv.DictReader(f)

        #skipping the extra metadata
        for i in range(3):
            l = reader.next()

        # processing file
        for line in reader:
            # calling function to fix the area value
            if "areaLand" in line:
                line["areaLand"] = fix_area(line["areaLand"])
            data.append(line)

    return data


def test():
    data = process_file(CITIES)

    print "Printing three example results:"
    for n in range(5,8):
        pprint.pprint(data[n]["areaLand"])

    assert data[3]["areaLand"] == None
    assert data[8]["areaLand"] == 55166700.0
    assert data[20]["areaLand"] == 14581600.0
    assert data[33]["areaLand"] == 20564500.0


if __name__ == "__main__":
    test()
