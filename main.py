import csv
import datetime

import pandas as pd
from dateutil import parser
from pandas import DataFrame
from pymongo import MongoClient, DESCENDING


def get_database():
    connection_string = "mongodb://localhost:27017"
    client = MongoClient(connection_string)
    return client["apps"]


def get_collection():
    db = get_database()
    return db["apps"]


def parse(item: dict) -> dict:
    res: dict = {"name": item["name"], "app_name": item["app_name"]}
    if item["rating"] != "":
        res["rating"] = float(item["rating"])
    if item["comment"] != "":
        res["comment"] = item["comment"]
        res["timestamp"] = parser.parse(item["comment_timestamp"])
    return res


def read_data():
    with open("data.csv", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        return [parse(row) for row in reader]


def create():
    item_1 = {
        "name": "John",
        "app_name": "com.google.example"
    }
    item_2 = {
        "name": "John",
        "app_name": "com.example.youtube",
        "rating": 4.5
    }
    collection.insert_many([item_1, item_2])
    item_3 = {
        "name": "John",
        "app_name": "com.example.example",
        "rating": 5.0,
        "comment": "The best app ever!",
        "timestamp": parser.parse("2024-11-12T00:00:00.000Z")
    }
    collection.insert_one(item_3)
    items = read_data()
    collection.insert_many(items)


def find_all():
    items = collection.find()
    print(DataFrame(items))


def find_all_by_app(app_name: str):
    items = collection.find({
        "app_name": app_name,
        "comment": {"$exists": True}
    })
    print(DataFrame(items))


def find_all_by_app_rating(app_name: str, rating: float):
    items = collection.find({
        "app_name": app_name,
        "comment": {"$exists": True},
        "rating": {"$exists": True, "$lte": rating}
    })
    print(DataFrame(items))


def find_all_by_app_timestamp(app_name: str, timestamp):
    items = collection.find({
        "app_name": app_name,
        "timestamp": {"$exists": True, "$gte": timestamp}
    }, {"name": 1, "app_name": 1, "comment": 1, "timestamp": 1})
    print(DataFrame(items))


def find_avg_ratings_by_app():
    items = collection.aggregate([
        {"$match": {"rating": {"$exists": True}}},
        {"$group": {"_id": "$app_name", "avg_rating": {"$avg": "$rating"}}}
    ])
    print(DataFrame(items))


def find_avg_rating_by_app(app_name: str):
    item = collection.aggregate([
        {"$match": {"app_name": app_name, "rating": {"$exists": True}}},
        {"$group": {"_id": "$app_name", "avg_rating": {"$avg": "$rating"}}}
    ])
    print(DataFrame(item))


def find_all_by_user(name: str):
    items = (collection.find({"name": name})
             .sort([("timestamp", DESCENDING), ("rating", DESCENDING)]))
    print(DataFrame(items))


def edit_comment(name: str, app_name: str, comment: str):
    result = collection.update_one(
        {"name": name, "app_name": app_name},
        {"$set": {
            "comment": comment,
            "timestamp": datetime.datetime.now()
        }}
    )
    print(result)


def edit_rating(name: str, app_name: str, rating: float):
    result = collection.update_one(
        {"name": name, "app_name": app_name},
        {"$set": {"rating": rating}}
    )
    print(result)


def delete_review(name: str, app_name: str):
    result = collection.delete_one({"name": name, "app_name": app_name})
    print(result)


if __name__ == "__main__":
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", None)
    collection = get_collection()
    # create()
    # find_all()
    # find_all_by_app("com.weather.weatherpro")
    # find_all_by_app_rating("com.weather.weatherpro", 2)
    # find_all_by_app_timestamp(
    #     "com.weather.weatherpro",
    #     datetime.datetime.now() - datetime.timedelta(days=365)
    # )
    # find_avg_ratings_by_app()
    # find_avg_rating_by_app("com.weather.weatherpro")
    # find_all_by_user("Bob")
    # edit_rating("Bob", "com.fit.fitbuddy", 3.5)
    # edit_comment("Bob", "com.fit.fitbuddy", "Love the interface!")
    # delete_review("Bob", "com.fit.fitbuddy")
    find_all()