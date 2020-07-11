#!/usr/bin/env python
"""
$unwind: expand array to multiple records

cities infobox dataset

Answer the question:  Which region or district in India contains the most cities?
(Make sure that the count of cities is stored in a field named 'count';
see the assertions at the end of the script.)

One thing to note about the cities data is that the "isPartOf" field contains an
array of regions or districts in which a given city is found.

Your code will be run against a MongoDB instance that we have provided.
If you want to run this code locally on your machine, you have to install MongoDB, 
download and insert the dataset.
"""

def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

def make_pipeline():
    pipeline = [{"$match": {"country": {"$eq": "India"}}},
                {"$unwind": "$isPartOf"},
                {"$group": {"_id": "$isPartOf", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 1}]
    return pipeline

def aggregate(db, pipeline):
    return [doc for doc in db.cities.aggregate(pipeline)]

if __name__ == '__main__':
    db = get_db('examples')
    pipeline = make_pipeline()
    result = aggregate(db, pipeline)
    print "Printing the first result:"
    import pprint
    pprint.pprint(result[0])
    assert result[0]["_id"] == "Uttar Pradesh"
    assert result[0]["count"] == 623
