#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This exercise uss US Patent database. The patent.data file is a small excerpt
of much larger datafiles that are available for download from US Patent website.
These files are pretty large ( >100 MB each). The original file is ~600MB large,
you might not be able to open it in a text editor.

The data itself is in XML, however there is a problem with how it's formatted.
The problem is that the gigantic file is actually not a valid XML, because it
has several root elements, and XML declarations. It is actually a collection of
a lot of concatenated XML documents.

NOTE: You do not need to correct the error.

Your task is to split the file into separate documents, so that you can process
the resulting files as valid XML documents.
"""
# https://docs.python.org/3/library/xml.etree.elementtree.html
# https://lxml.de/tutorial.html

import xml.etree.ElementTree as ET
from lxml import etree

PATENTS = 'patent.data'

def get_root(fname):
    tree = ET.parse(fname)
    return tree.getroot()


def split_file(filename):
    """
    Split the input file into separate files, each containing a single patent.
    As a hint - each patent declaration starts with the same line that was
    causing the error found in the previous exercises.

    The new files should be saved with filename in the following format:
    "{}-{}".format(filename, n) where n is a counter, starting from 0.

    n = 0
    context = ET.iterparse(PATENTS, events=('end', ))
    for event, elem in context:
        if elem.tag == "us-patent-grant":
            #elem.get('file')
            print(ET.tostring(elem))
            print("/n")
            f = open("{}-{}".format(PATENTS, n), 'w')
            f.write('<?xml version="1.0" encoding="UTF-8"?>')
            f.write('<!DOCTYPE us-patent-grant SYSTEM "us-patent-grant-v44-2013-05-16.dtd" [ ]>')
            f.write(ET.tostring(elem))
            n += 1
    """
    n = 0
    accumulated_xml = []
    with open(PATENTS) as temp:
        while True:
            line = temp.readline()
            if line:
                if line.startswith('<?xml'):
                    if accumulated_xml:
                        f = open("{}-{}".format(PATENTS, n), 'w')
                        tree = etree.fromstring(''.join(accumulated_xml))
                        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                        f.write('<!DOCTYPE us-patent-grant SYSTEM "us-patent-grant-v44-2013-05-16.dtd" [ ]>\n')
                        f.write(etree.tostring(tree, pretty_print=True))
                        file = open("{}-{}".format(PATENTS, n), 'r')
                        print(file.read())
                        accumulated_xml = []
                        n += 1
                else:
                    accumulated_xml.append(line.strip())
            else:
                f = open("{}-{}".format(PATENTS, n), 'w')
                tree = etree.fromstring(''.join(accumulated_xml))
                f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                f.write('<!DOCTYPE us-patent-grant SYSTEM "us-patent-grant-v44-2013-05-16.dtd" [ ]>\n')
                f.write(etree.tostring(tree, pretty_print=True))
                break


def test():
    split_file(PATENTS)
    for n in range(4):
        try:
            fname = "{}-{}".format(PATENTS, n)
            f = open(fname, "r")
            if not f.readline().startswith("<?xml"):
                print "You have not split the file {} in the correct boundary!".format(fname)
            f.close()
        except:
            print "Could not find file {}. Check if the filename is correct!".format(fname)


test()
