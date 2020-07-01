#!/usr/bin/env python
"""
Other Operators
$exists: 0 or 1
$regex
https://docs.mongodb.com/manual/reference/operator/query/regex/
$in
$all
$set

Your task is to write a query that will return all cars manufactured by
"Ford Motor Company" that are assembled in Germany, United Kingdom, or Japan.

Your code will be run against a MongoDB instance that we have provided.
If you want to run this code locally on your machine,
you have to install MongoDB, download and insert the dataset.
"""

db.cities.find({"governmentType": {"$exists": 1}}).pretty()

# motto is exactly "friendship"
db.cities.find({"motto": {"$regex": "friendship"}}).count()

db.cities.find({"motto": {"$regex": "[Ff]riendship"}}).count()

# motto contains either friendship or pride, F and P can be capital or lower case
db.cities.find({"motto": {"$regex": "[Ff]riendship|[Pp]ride"}}, {"motto": 1, "_id": 0}).pretty()

# if find 1980 is in modelYears array, output the city
db.cities.find({"modelYears": 1980}).pretty()

# if find any one of 1965, 1966, 1967 in modelYears array, count the city
db.cities.find({"modelYears": {"$in": [1965, 1966, 1967]}}).count()

# if find all of 1965, 1966, 1967 in modelYears array, count the city
db.cities.find({"modelYears": {"$all": [1965, 1966, 1967]}}).count()

# hashtags is sub-field of entities, text is sub-field of hashtags
db.tweets.find({"entities.hashtags": {"$ne": []}}, {"entities.hashtags.text": 1, "_id": 0}).count()



def in_query():
    query = {"manufacturer" : "Ford Motor Company",
            "assembly": {"$in": ['Germany', 'United Kingdom', 'Japan']}}

    return query


def get_db():
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client.examples
    return db


if __name__ == "__main__":

    db = get_db()
    query = in_query()
    autos = db.autos.find(query, {"name":1, "manufacturer":1, "assembly": 1, "_id":0})

    print "Found autos:", autos.count()
    import pprint
    for a in autos:
        pprint.pprint(a)



# find one record
city = db.cities.find_one({"name": "Munchen", "country": "Germany"})
city["isoCountryCode"] = "DEU"
# update city using save (replace old one or insert if not exists)
db.cities.save(city)

# update only update the first record by default
# update isoCountryCode to DEU using $set
city = db.cities.update({"name": "Munchen", "country": "Germany"},
                        {"$set": {"isoCountryCode": "DEU"}})
# if isoCountryCode exists, remove it, if not exists, doc will not change
city = db.cities.update({"name": "Munchen", "country": "Germany"},
                        {"$unset": {"isoCountryCode": ""}})

# don't do this, which will update each city data that match the first {} to only 2 fields - id and isoCountryCode
db.cities.update({"name": "Munchen", "country": "Germany"},
                {"isoCountryCode": ""})


# update multiple docs
db.cities.update({"country": "Germany"},
                {"$set": {"isoCountryCode": "DEU"}},
                multi = True)


# remove all cities one by one
db.cities.remove()
# more efficient way to delete whole cities collection
db.cities.drop()
# remove records whose city name is Chicago
db.cities.remove({"name": "Chicago"})
db.cities.find({"name": "Chicago"})
# remove all cities that don't have city name
db.cities.remove({"name": {"$exists": 0}})
