#!/usr/bin/env python
"""
Use an aggregation query to answer the following question.

Extrapolating from an earlier exercise in this lesson, find the average
regional city population for all countries in the cities collection. What we
are asking here is that you first calculate the average city population for each
region in a country and then calculate the average of all the regional averages
for a country.

As a hint, _id fields in group stages need not be single values. They can
also be compound keys (documents composed of multiple fields). You will use the
same aggregation operator in more than one stage in writing this aggregation
query.
https://docs.mongodb.com/manual/tutorial/aggregation-zip-code-data-set/

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
    pipeline = [{"$unwind": "$isPartOf"},
                {"$group": {"_id": {"country": "$country",
                                    "region": "$isPartOf"},
                            "avg_city_population": {"$avg": "$population"}}},
                {"$group": {"_id" : "$_id.country",
                            "avgRegionalPopulation": {"$avg": "$avg_city_population"}}}]
    return pipeline

def aggregate(db, pipeline):
    return [doc for doc in db.cities.aggregate(pipeline)]

if __name__ == '__main__':
    db = get_db('examples')
    pipeline = make_pipeline()
    result = aggregate(db, pipeline)
    import pprint
    if len(result) < 150:
        pprint.pprint(result)
    else:
        pprint.pprint(result[:100])
    key_pop = 0
    for country in result:
        if country["_id"] == 'Lithuania':
            assert country["_id"] == 'Lithuania'
            assert abs(country["avgRegionalPopulation"] - 14750.784447977203) < 1e-10
            key_pop = country["avgRegionalPopulation"]
    assert {'_id': 'Lithuania', 'avgRegionalPopulation': key_pop} in result
