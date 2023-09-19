#!/usr/bin/env python
# Zodpovězení dotazů a prezentace výsledků.

import matplotlib.pyplot as plt
import mysql.connector
import pandas as pd
from sys import argv
import settings

db = mysql.connector.connect(
    host=settings.MySQLHost,
    user=settings.MySQLUser,
    password=settings.MySQLPassword,
    database=settings.MySQLDatabase
)
cursor = db.cursor()


def dotazA(addr):
    sql = "SELECT download, upload, ping FROM tests NATURAL JOIN users WHERE ip LIKE %s"
    ipSearch = addr
    if addr[-1] == '.' or addr.count('.') < 2:
        ipSearch += "%"

    cursor.execute(sql, (ipSearch,))
    columns = ['download', 'upload', 'ping']
    df = pd.DataFrame(cursor.fetchall(), columns=columns)

    try:
        for column in columns:
            df.boxplot(column=column)
            plt.title("IP prefix: " + addr)
            plt.show()
    except KeyError:
        print("Nebyla nalezena žádná data s IP prefixem ", addr)


def vlastniDotaz():
    columns = ['download', 'upload', 'ping']

    for column in columns:
        sql = "SELECT " + column + ", technology FROM tests NATURAL JOIN users"
        cursor.execute(sql)
        data = cursor.fetchall()

        df = pd.DataFrame(data).groupby(1)
        df.boxplot(subplots=False, rot=20, fontsize=10)

        plt.title(column)
        plt.show()


if __name__ == '__main__':
    if len(argv) == 1:
        vlastniDotaz()
    else:
        dotazA(argv[1])
