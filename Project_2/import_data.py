#!/usr/bin/env python
# Import csv tabulky do MongoDB

import pandas as pd
from pymongo import MongoClient
from sys import argv
import settings

if __name__ == '__main__':
    if len(argv) != 2:
        print("Použití: import_data.py soubor_s_daty_pro_import.csv")
        exit(1)

    client = MongoClient(settings.MongoDBHost, settings.MongoDBPort)
    db = client.upa_db
    collection = db.speed_collection

    try:
        data = pd.read_csv(argv[1], header=0)
    except FileNotFoundError as e:
        print("Použití: import_data.py soubor_s_daty_pro_import.csv")
        print(str(e))
        exit(1)

    # Smazání neplatných dat
    data = data.loc[~data['implausible'], :]
    data = data.loc[data['tags'] == '{}', :]
    data = data.drop(columns=['implausible', 'tags'])

    data = data[data['download_kbit'].notna()]
    data = data[data['upload_kbit'].notna()]
    data = data[data['ping_ms'].notna()]

    collection.insert_many(data.to_dict('records'))
    print("Data importována!")
