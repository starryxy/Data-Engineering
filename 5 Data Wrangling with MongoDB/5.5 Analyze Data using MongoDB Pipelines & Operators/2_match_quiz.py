#!/usr/bin/env python
"""
Use an aggregation query to answer the following question.

What is the most common city name in our cities collection?

Your first attempt probably identified None as the most frequently occurring
city name. What that actually means is that there are a number of cities
without a name field at all. It's strange that such documents would exist in
this collection and, depending on your situation, might actually warrant
further cleaning.

To solve this problem the right way, we should really ignore cities that don't
have a name specified. As a hint ask yourself what pipeline operator allows us
to simply filter input? How do we test for the existence of a field?

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
    pipeline = [{"$match": {"name": {"$ne": None}}},
                {"$group": {"_id" : "$name",
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
    assert result[0] == {'_id': 'Shahpur', 'count': 6}
