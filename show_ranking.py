#!/usr/bin/env python3

import numpy as np
import sys
import os
import psycopg2
import datetime
from tqdm import tqdm

class SelectPhrase:
    @classmethod
    def generate(self, reference):
        query = 'SELECT ' + reference.cols.strip() + ' FROM ' + reference.table.strip()

        if reference.conditions.strip():
            query += ' WHERE ' + reference.conditions.strip()

        if reference.order.strip():
            query += ' ORDER BY ' + reference.order.strip()

        if reference.limit.strip():
            query += ' LIMIT ' + reference.limit.strip()

        return query

class RatingReference:
    __cols = 'year, monthday, jyocd, kaiji, nichiji, racenum, kettonum, rating'

    def __init__(self, table):
        self.table      = table
        self.cols       = self.__cols
        self.conditions = ''
        self.order      = 'rating DESC'
        self.limit      = '10'

    @classmethod
    def index(self, colname):
        return self.__cols.strip().split(', ').index(colname)

class RatingReader:
    @classmethod
    def load_data(self, connection, table):
        with connection.cursor() as cur:
            query = SelectPhrase.generate(RatingReference(table))
            cur.execute(query)
            rating_list = cur.fetchall()

        return rating_list

if __name__ == "__main__":
    if (len(sys.argv) != 2):
        print('usage: show_ranking.py <tablename>')

    try:
        connection_processed = psycopg2.connect(os.environ.get('DB_UMA_PROCESSED'))
    except:
        print('psycopg2: opening connection faied')
        sys.exit(0)

    try:
        rating_list = RatingReader.load_data(connection_processed, sys.argv[1])
    except RuntimeError as e:
        print(e)

    for row in rating_list:
        print("%s%s:%s:%s" % (row[RatingReference.index('year')], row[RatingReference.index('monthday')], row[RatingReference.index('kettonum')], row[RatingReference.index('rating')]) )

    connection_processed.close()

