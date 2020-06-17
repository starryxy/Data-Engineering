#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Your task is to process the HTML using BeautifulSoup,
# extract the carriers and airport lists,
# extract the hidden form field values for "__EVENTVALIDATION" and "__VIEWSTATE"
# and set the appropriate values in the data dictionary.

# https://www.crummy.com/software/BeautifulSoup/bs4/doc/

from bs4 import BeautifulSoup
import requests
import json

html_page1 = "page_source.html"
html_page2 = "options.html"

s = requests.Session()
r = s.get("http://www.transtats.bts.gov/Data_Elements.aspx?Data=2")

data = {}

def extract_carriers(page):
    carrier_data = []

    with open(page, "r") as html:
        soup = BeautifulSoup(html, "lxml")
        carrier = soup.find(id="CarrierList")
        for option in carrier.find_all('option'):
            if "All" in option['value']:
                continue
            carrier_data.append(option['value'])
        print(carrier_data)

    return carrier_data


def extract_airports(page):
    airport_data = []

    with open(page, "r") as html:
        soup = BeautifulSoup(html, "lxml")
        airport = soup.find(id="AirportList")
        for option in airport.find_all('option'):
            if "All" in option['value']:
                continue
            airport_data.append(option['value'])
        print(airport_data)

    return airport_data


def extract_data(page):
    data = {}

    with open(page, "r") as html:
        soup = BeautifulSoup(html, "lxml")
        ev = soup.find(id="__EVENTVALIDATION")
        print(ev)
        data["eventvalidation"] = ev["value"]
        vs = soup.find(id="__VIEWSTATE")
        print(vs)
        data["viewstate"] = vs["value"]
        print(data)

    return data


carrier_data = extract_carriers(html_page2)
airport_data = extract_airports(html_page2)
data = extract_data(html_page1)
data["carrier"] = carrier_data
data["airport"] = airport_data


def make_request(data):
    viewstate = data["viewstate"]
    eventvalidation = data["eventvalidation"]
    carrier = data["carrier"]
    airport = data["airport"]

    r = s.post("https://www.transtats.bts.gov/Data_Elements.aspx?Data=2",
           data = (
                   ("__EVENTTARGET", ""),
                   ("__EVENTARGUMENT", ""),
                   ("__VIEWSTATE", viewstate),
                   ("__VIEWSTATEGENERATOR",viewstategenerator),
                   ("__EVENTVALIDATION", eventvalidation),
                   ("CarrierList", carrier),
                   ("AirportList", airport),
                   ("Submit", "Submit")
                  ))

    return r.text


output = make_request(data)
f = open('virgin_and_logan_airport.html', 'w')
f.write(output)
