#!/usr/bin/env python
"""
Your task is to find all autos where the manufacturer field matches "Porsche"
and "class" is "mid-size car", and output only "name".

Your code will be run against a MongoDB instance that we have provided.
If you want to run this code locally on your machine,
you have to install MongoDB and download and insert the dataset.
"""

def porsche_query():
    query = {"manufacturer": "Porsche", "class": "mid-size car"}
    # instead of showing all results, only show name, mark 0 to id so id will not show
    projection = {"_id": 0, "name": 1}
    return query


def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

def find_porsche(db, query):
    return db.autos.find(query)


if __name__ == "__main__":
    db = get_db('examples')
    query = porsche_query()
    results = find_porsche(db, query)

    print "Printing first 3 results\n"
    import pprint
    for car in results[:3]:
        pprint.pprint(car)
