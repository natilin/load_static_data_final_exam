from typing import List
from app.db.mongo_database import events_collection


def insert_all_data(data: List):
    events_collection.insert_many(data)