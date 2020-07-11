#!/usr/bin/env python
"""
-- Output users that mentioned the most unique users
pipeline = [{"$unwind": "$entities.user_mentions"},
            {"$group": {"_id": "$user.screen_name",
                        "mset": {"$addToSet": "$entities.user_mentions.screen_name"}}},
            {"$unwind": "$mset"},
            {"$group": {"_id" : "$_id",
                        "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}]


cities infobox dataset

What is the average city population for a region in India? 
Calculate your answer by first finding the average population of cities in each
region and then by calculating the average of the regional averages.

Hint: If you want to accumulate using values from all input documents to a group
stage, you may use a constant as the value of the "_id" field. For example,
    { "$group" : {"_id" : "India Regional City Population Average",
      ... }

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
                {"$group": {"_id": "$isPartOf", "avg_city_population": {"$avg": "$population"}}},
                {"$group": {"_id" : "India Regional City Population Average",
                            "avg": {"$avg": "$avg_city_population"}}}]
    return pipeline

def aggregate(db, pipeline):
    return [doc for doc in db.cities.aggregate(pipeline)]


if __name__ == '__main__':
    db = get_db('examples')
    pipeline = make_pipeline()
    result = aggregate(db, pipeline)
    assert len(result) == 1
    assert abs(result[0]["avg"] - 201128.0241546919) < 10 ** -8
    import pprint
    pprint.pprint(result)
