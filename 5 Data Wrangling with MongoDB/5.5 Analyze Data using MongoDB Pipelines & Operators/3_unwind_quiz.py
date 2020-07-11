#!/usr/bin/env python
"""
Use an aggregation query to answer the following question.

Which Region in India has the largest number of cities with longitude between
75 and 80?

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
    pipeline = [{"$match": {"country": {"$eq": "India"},
                            "lon": {"$gt": 75, "$lt": 80}}},
                {"$unwind": "$isPartOf"},
                {"$group": {"_id": "$isPartOf",
                            "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 1}]
    return pipeline

def aggregate(db, pipeline):
    return [doc for doc in db.cities.aggregate(pipeline)]

if __name__ == '__main__':
    db = get_db('examples')
    pipeline = make_pipeline()
    result = aggregate(db, pipeline)
    import pprint
    pprint.pprint(result[0])
    assert len(result) == 1
    assert result[0]["_id"] == 'Tamil Nadu'
    assert result[0]["count"] == 424
