
import json
import requests

BASE_URL = "http://musicbrainz.org/ws/2/"
ARTIST_URL = BASE_URL + "artist/"


# query parameters for requests.get function
# some starter parameters
query_type = {  "simple": {},
                "atr": {"inc": "aliases+tags+ratings"},
                "aliases": {"inc": "aliases"},
                "releases": {"inc": "releases"}}


def query_site(url, params, uid="", fmt="json"):
    """
    This is the main function for making queries to the musicbrainz API.
    The query should return a json document.
    """
    params["fmt"] = fmt
    r = requests.get(url + uid, params=params)
    print("requesting", r.url)

    if r.status_code == requests.codes.ok:
        return r.json()
    else:
        r.raise_for_status()


def query_by_name(url, params, name):
    """
    This adds an artist name to the query parameters
    before making an API call to the function above.
    """
    params["query"] = "artist:" + name
    return query_site(url, params)


def pretty_print(data, indent=4):
    """
    Format output to be more readable.
    """
    if type(data) == dict:
        print(json.dumps(data, indent=indent, sort_keys=True))
    else:
        print(data)


def main():

    # Query for information in the database about bands named First Aid Kit
    results = query_by_name(ARTIST_URL, query_type["simple"], "First Aid Kit")
    # Count number of bands
    print("\nCOUNT:")
    print(len(results["artists"]))

    # Query for information in the database about bands named Queen
    results = query_by_name(ARTIST_URL, query_type["simple"], "Queen")
    print(results["artists"][0]["begin-area"]["name"])
    # Find begin-area name
    n = len(results["artists"])
    for i in range(n):
        if "begin-area" not in results["artists"][i]:
            continue
        if "name" not in results["artists"][i]["begin-area"]:
            continue

        print("\ni:", i)
        print("BEGIN-AREA NAME:")
        print(results["artists"][i]["begin-area"]["name"])

    # Query for information in the database about bands named Beatles
    results = query_by_name(ARTIST_URL, query_type["simple"], "Beatles")
    print(results["artists"][0]["aliases"][22]["locale"])
    print(results["artists"][0]["aliases"][22]["name"])
    # Find spanish alias name
    n = len(results["artists"])
    for i in range(n):
        if "aliases" not in results["artists"][i]:
            continue

        m = len(results["artists"][i]["aliases"])
        for j in range(m):
            if "locale" not in results["artists"][i]["aliases"][j]:
                continue

            if results["artists"][i]["aliases"][j]["locale"] == "es":
                print("\ni:", i)
                print("j:", j)
                print("SPANISH ALIASES:")
                print(results["artists"][i]["aliases"][j]["name"])

    # Query for information in the database about bands named Nirvana
    results = query_by_name(ARTIST_URL, query_type["simple"], "Nirvana")
    print("\nDISAMBIGUATION:")
    print(results["artists"][0]["disambiguation"])
    # Find disambiguation
    n = len(results["artists"])
    for i in range(n):
        if "tags" not in results["artists"][i]:
            continue

        m = len(results["artists"][i]["tags"])
        for j in range(m):
            if results["artists"][i]["tags"][j]["name"] == "kurt cobain":
                print("\ni:", i)
                print("j:", j)
                print("DISAMBIGUATION:")
                print(results["artists"][i]["disambiguation"])

    # Query for information in the database about bands named One Direction
    results = query_by_name(ARTIST_URL, query_type["simple"], "One Direction")
    print("\nYEAR:")
    print(results["artists"][0]["life-span"]["begin"])
    # Find formed year
    n = len(results["artists"])
    for i in range(n):
        if "life-span" not in results["artists"][i]:
            continue
        if "begin" not in results["artists"][i]["life-span"]:
            continue

        print("\ni:", i)
        print("YEAR:")
        print(results["artists"][i]["life-span"]["begin"])


if __name__ == '__main__':
    main()
