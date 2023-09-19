#!/usr/bin/env python
# Převod dat to relační databáze
import mysql.connector
import math
from pymongo import MongoClient

import settings

if __name__ == '__main__':
    try:
        rel_db = mysql.connector.connect(
            host=settings.MySQLHost,
            user=settings.MySQLUser,
            password=settings.MySQLPassword,
            database=settings.MySQLDatabase
        )
        rel_cursor = rel_db.cursor()
    except mysql.connector.Error:
        rel_db = mysql.connector.connect(
            host=settings.MySQLHost,
            user=settings.MySQLUser,
            password=settings.MySQLPassword
        )
        rel_cursor = rel_db.cursor()
        rel_cursor.execute("CREATE DATABASE " + settings.MySQLDatabase)

    client = MongoClient(settings.MongoDBHost, settings.MongoDBPort)
    mongo_db = client.upa_db
    mongo_collection = mongo_db.speed_collection

    rel_cursor.execute("USE " + settings.MySQLDatabase)

    try:
        rel_cursor.execute("DROP TABLE tests")
    except mysql.connector.Error:
        pass

    try:
        rel_cursor.execute("DROP TABLE users")
    except mysql.connector.Error:
        pass

    rel_cursor.execute(
        'CREATE TABLE users '
        '(user_uuid VARCHAR(40) PRIMARY KEY, ip VARCHAR(39), technology VARCHAR(255), network_name VARCHAR(255))'
    )
    rel_cursor.execute(
        'CREATE TABLE tests '
        '(test_uuid VARCHAR(40) PRIMARY KEY, user_uuid VARCHAR(40), download INT, upload INT, ping DOUBLE,'
        ' signal_strength DOUBLE, platform VARCHAR(255),'
        ' FOREIGN KEY (user_uuid) REFERENCES users(user_uuid))'
    )
    sql_users = 'INSERT INTO users (user_uuid, ip, technology, network_name) VALUES (%s, %s, %s, %s)'
    sql_tests = 'INSERT INTO tests (test_uuid, user_uuid, download, upload, ping, signal_strength, platform)' \
                'VALUES (%s, %s, %s, %s, %s, %s, %s)'

    i = 0
    values = []
    print('Vkládání uživatelů...', end='')
    for user in mongo_collection.aggregate([
        {'$group': {"_id": "$open_uuid",
                    'ip_anonym': {'$first': '$ip_anonym'},
                    'cat_technology': {'$first': '$cat_technology'},
                    'network_name': {'$first': '$network_name'}}}
    ]):
        i += 1
        network = user['network_name'] if type(user['network_name']) == str else None
        values.append([user['_id'], user['ip_anonym'], user['cat_technology'], network])
        if i == 10000:
            rel_cursor.executemany(sql_users, values)
            rel_db.commit()
            i = 0
            values = []
    if i != 0:
        rel_cursor.executemany(sql_users, values)
        rel_db.commit()
    print('Dokončeno.')

    i = 0
    values = []

    print('Vkládání testů...', end='')
    for data in mongo_collection.find():
        i += 1
        signal = data['lte_rsrp'] if not math.isnan(data['lte_rsrp']) else None
        platform = data['platform'] if type(data['platform']) == str else None
        values.append(
            [data['open_test_uuid'], data['open_uuid'], data['download_kbit'], data['upload_kbit'], data['ping_ms'],
             signal, platform])
        if i == 10000:
            rel_cursor.executemany(sql_tests, values)
            rel_db.commit()
            i = 0
            values = []

    if i != 0:
        rel_cursor.executemany(sql_tests, values)
        rel_db.commit()

    print('Dokončeno.')
