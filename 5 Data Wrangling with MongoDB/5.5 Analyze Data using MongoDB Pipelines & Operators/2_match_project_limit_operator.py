#!/usr/bin/env python
"""
$project: only output certain fields, insert computed fields,
            rename fields, create fields holding subfields
$match: filter certain fields
$skip: skip first x records
$limit: select first x records

https://docs.mongodb.com/manual/reference/operator/aggregation/


Write an aggregation query to answer this question:

Of the users in the "Brasilia" timezone who have tweeted 100 times or more,
who has the largest number of followers?

- Time zone is found in the "time_zone" field of the user object in each tweet
- The number of tweets for each user is found in the "statuses_count" field
- Output followers to friends ratio
- Your aggregation query should return something like the following:
{u'ok': 1.0,
 u'result': [{u'_id': ObjectId('52fd2490bac3fa1975477702'),
                  u'followers': 2597,
                  u'screen_name': u'marbles',
                  u'tweets': 12334,
                  u'ratio': 123.5}]}

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
    pipeline = [{"$match": {"user.time_zone": {"$eq": "Brasilia"},
                            "user.statuses_count": {"$gte": 100}}},
                {"$project": {"followers": "$user.followers_count",
                              "screen_name": "$user.screen_name",
                              "tweets": "$user.statuses_count",
                              "ratio": {"$divide": ["$user.followers_count",
                                                    "$user.friends_count"]}}},
                {"$sort": {"followers": -1}},
                {"$limit": 1}]
    return pipeline

def aggregate(db, pipeline):
    return [doc for doc in db.tweets.aggregate(pipeline)]


if __name__ == '__main__':
    db = get_db('twitter')
    pipeline = make_pipeline()
    result = aggregate(db, pipeline)
    import pprint
    pprint.pprint(result)
    assert len(result) == 1
    assert result[0]["followers"] == 17209
