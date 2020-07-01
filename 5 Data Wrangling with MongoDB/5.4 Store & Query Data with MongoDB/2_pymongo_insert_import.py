#!/usr/bin/env python
"""
Insert the automobile data into the 'autos' collection.
The data variable that is returned from the process_file function is a list of dictionaries.
"""

from autos import process_file


def insert_autos(infile, db):
    data = process_file(infile)

    print("cnt before: ", db.autos.find().count())

    for a in data:
        db.autos.insert(a)

    print("cnt after: ", db.autos.find().count())


if __name__ == "__main__":
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27017")
    db = client.examples

    insert_autos('autos-small.csv', db)
    print db.autos.find_one()


"""
command line to import JSON file into MongoDB
"""
# -d database name, -c collection name data should be stored in, --file file to import
mongoimport -d examples -c myautos --file autos.json
