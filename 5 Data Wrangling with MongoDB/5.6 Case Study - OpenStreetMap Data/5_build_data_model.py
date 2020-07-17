#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Your task is to wrangle the data and transform the shape of the data
into a data model. The output should be a list of dictionaries that look like this:

{
"id": "2406124091",
"type: "node",
"visible":"true",
"created": {
          "version":"2",
          "changeset":"17206049",
          "timestamp":"2013-08-03T16:43:42Z",
          "user":"linuxUser16",
          "uid":"1219059"
        },
"pos": [41.9757030, -87.6921867],
"address": {
          "housenumber": "5157",
          "postcode": "60625",
          "street": "North Lincoln Ave"
        },
"amenity": "restaurant",
"cuisine": "mexican",
"name": "La Cabana De Don Luis",
"phone": "1 (773)-271-5176"
}

Parse the map file, and shape data of each the element into a dictionary.
Save the shaped data in a file so you could use mongoimport to import it into MongoDB.

In particular the following things should be done:
- you should process only 2 types of top level tags: "node" and "way"
- all attributes of "node" and "way" should be turned into regular key/value pairs, except:
    - attributes in the CREATED array should be added under a key "created"
    - attributes for latitude and longitude should be added to a "pos" array,
      for use in geospacial indexing.
      Make sure the values inside "pos" array are floats and not strings.
- if the second level tag "k" value contains problematic characters, it should be ignored
- if the second level tag "k" value starts with "addr:", it should be added to a dictionary "address"
- if the second level tag "k" value does not start with "addr:", but contains ":", you can
  process it in a way that you feel is best. For example, you might split it into a two-level
  dictionary like with "addr:", or otherwise convert the ":" to create a valid key.
- if there is a second ":" that separates the type/direction of a street,
  the tag should be ignored, for example:

<tag k="addr:housenumber" v="5158"/>
<tag k="addr:street" v="North Lincoln Avenue"/>
<tag k="addr:street:name" v="Lincoln"/>
<tag k="addr:street:prefix" v="North"/>
<tag k="addr:street:type" v="Avenue"/>
<tag k="amenity" v="pharmacy"/>

  should be turned into:

{...
"address": {
    "housenumber": 5158,
    "street": "North Lincoln Avenue"
}
"amenity": "pharmacy",
...
}

- for "way" specifically:

  <nd ref="305896090"/>
  <nd ref="1719825889"/>

should be turned into
"node_refs": ["305896090", "1719825889"]
"""
import xml.etree.cElementTree as ET
from pprint import pprint
import re
import codecs
import json

lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
two_colons = re.compile(r'^([a-z]|_)*:([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\t\r\n]')

CREATED = [ "version", "changeset", "timestamp", "user", "uid"]


def shape_element(element):
    node = {}
    if element.tag == "node" or element.tag == "way" :
        node["type"] = element.tag
        created_dict = {}
        pos = []
        address = {}
        node_refs = []
        for tag in element.iter():
            if tag.tag == "tag":
                x = lower_colon.search(tag.attrib["k"])
                y = two_colons.search(tag.attrib["k"])
                if x:
                    if "addr:" in tag.attrib["k"]:
                        address[tag.attrib["k"].split(":", 1)[1]] = tag.attrib["v"]
                    else:
                        node[tag.attrib["k"].replace(":", "_")] = tag.attrib["v"]
                elif y:
                    pass
                else:
                    node[tag.attrib["k"]] = tag.attrib["v"]
            else:
                for k in tag.attrib:
                    z = problemchars.search(tag.attrib[k])
                    if z:
                        pass
                    else:
                        if k in CREATED:
                            created_dict[k] = tag.attrib[k]
                        elif k == "lat":
                            pos.insert(0, float(tag.attrib[k]))
                        elif k == "lon":
                            pos.insert(1, float(tag.attrib[k]))
                        elif k == "ref":
                            node_refs.append(tag.attrib[k])
                        else:
                            node[k] = tag.attrib[k]

        if created_dict:
            node["created"] = created_dict
        if pos:
            node["pos"] = pos
        if address:
            node["address"] = address
        if node_refs:
            node["node_refs"] = node_refs

        pprint(node)
        return node
    else:
        return None


def process_map(file_in, pretty = False):
    file_out = "{0}.json".format(file_in)
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            el = shape_element(element)
            if el:
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
    return data

def test():
    # NOTE: if you are running this code on your computer to write to a JSON file, 
    # call the process_map procedure with pretty=False. Otherwise, mongoimport
    # might give you an error when you try to import the JSON file to MongoDB.
    # The pretty=True option adds additional spaces to the output, making it
    # significantly larger.
    data = process_map('example.osm', True)
    # pprint(data)

    correct_first_elem = {
        "id": "261114295",
        "visible": "true",
        "type": "node",
        "pos": [41.9730791, -87.6866303],
        "created": {
            "changeset": "11129782",
            "user": "bbmiller",
            "version": "7",
            "uid": "451048",
            "timestamp": "2012-03-28T18:31:23Z"
        }
    }
    assert data[0] == correct_first_elem
    assert data[-1]["address"] == {
                                    "street": "West Lexington St.",
                                    "housenumber": "1412"
                                      }
    assert data[-1]["node_refs"] == [ "2199822281", "2199822390",  "2199822392", "2199822369",
                                    "2199822370", "2199822284", "2199822281"]

if __name__ == "__main__":
    test()
