#!/usr/bin/env python
from pymongo import MongoClient
import settings

if __name__ == '__main__':
    client = MongoClient(settings.MongoDBHost, settings.MongoDBPort)
    db = client.upa_db

    collection = db.speed_collection
    collection.drop()
    print("Databáze vymazána!")
