from typing import List

import pymongo

from app.db.mongo_database import events_collection


def insert_all_data(data: List):
    events_collection.insert_many(data)

def create_index():
    events_collection.create_index("uuid", unique=True)
    events_collection.create_index("group_name")