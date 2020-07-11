#!/usr/bin/env python
"""
$group
$sort

Using the $group operator, your task is to identify most used applications for
creating tweets and return a list of one or more dictionary objects.

The tweets in our twitter collection have a field called "source". This field
describes the application that was used to create the tweet.
As a check on your query, 'web' is listed as the most frequently used application.
'Ubertwitter' is the second most used.
The number of counts should be stored in a field named 'count'.

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
    pipeline = [{"$group": {"_id": "$source", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}]
    return pipeline

def tweet_sources(db, pipeline):
    return [doc for doc in db.tweets.aggregate(pipeline)]

if __name__ == '__main__':
    db = get_db('twitter')
    pipeline = make_pipeline()
    result = tweet_sources(db, pipeline)
    import pprint
    pprint.pprint(result[0])
    assert result[0] == {u'count': 868, u'_id': u'web'}
