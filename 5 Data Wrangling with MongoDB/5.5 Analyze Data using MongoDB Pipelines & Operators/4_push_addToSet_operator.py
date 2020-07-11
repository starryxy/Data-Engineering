#!/usr/bin/env python
"""
$group Accumulator Operator
$first
$last
$max
$min
$avg

$push: aggregate all values into an array for each group
$addToSet: accumulate unique values into an array for each group

-- Output avg count of retweet by hashtag
pipeline = [{"$unwind": "$entities.hashtags"},
            {"$group": {"_id": "$entities.hashtags.text",
                        "retweet_avg": {"$avg": "$retweet_count"}}},
            {"$sort": {"retweet_avg": -1}}]

-- Output unique hashtags each user used in tweets
pipeline = [{"$unwind": "$entities.hashtags"},
            {"$group": {"_id": "$user.screen_name",
                        "unique_hashtags": {"$addToSet": "$entities.hashtags.text"}}},
            {"$sort": {"_id": -1}}]


Using an aggregation query, count the number of tweets for each user.
In the same $group stage, use $push to accumulate all the tweet texts for each
user. Limit output to the 5 users with the most tweets.

Your result documents should include only the fields:
"_id" (screen name of user),
"count" (number of tweets found for the user),
"tweet_texts" (a list of the tweet texts found for the user).

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
    pipeline = [{"$group": {"_id": "$user.screen_name",
                            "count": {"$sum": 1},
                            "tweet_texts": {"$push": "$text"}}},
                {"$sort": {"count": -1}},
                {"$limit": 5}]
    return pipeline

def aggregate(db, pipeline):
    return [doc for doc in db.twitter.aggregate(pipeline)]


if __name__ == '__main__':
    db = get_db('twitter')
    pipeline = make_pipeline()
    result = aggregate(db, pipeline)
    import pprint
    pprint.pprint(result)
    assert len(result) == 5
    assert result[0]["count"] > result[4]["count"]
    sample_tweet_text = u'Take my money! #liesguystell http://movie.sras2.ayorganes.com'
    assert result[4]["tweet_texts"][0] == sample_tweet_text
