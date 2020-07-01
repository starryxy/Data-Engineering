#!/usr/bin/env python
"""
Inequality Operators
$gt: greater than
$lt: less than
$gte: greater than or equal to
$lte: less than or equal to
$ne: not equal to

https://docs.mongodb.com/manual/reference/operator/query/

Your task is to write a query that will return all cities
that are founded in 21st century.

Your code will be run against a MongoDB instance that we have provided.
If you want to run this code locally on your machine,
you have to install MongoDB, download and insert the dataset.
"""

from datetime import datetime
    
def range_query():
    query = {"foundingDate": {"$gt": datetime(2001, 1, 1, 0, 0),
                              "$lt": datetime(2100, 1, 1, 0, 0)}}
    # query = {"population": {"$gt": 250000}}
    # name starting with X
    # query = {"name": {"$gte": "X", "$lt": "Y"}}
    # query = {"country": {"$ne": "United States"}}
    return query

def get_db():
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client.examples
    return db

if __name__ == "__main__":
    # For local use
    db = get_db()
    query = range_query()
    cities = db.cities.find(query)

    print "Found cities:", cities.count()

    import pprint
    pprint.pprint(cities[0])
