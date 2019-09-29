#!/usr/bin/env python3

import numpy as np
import os
import psycopg2
import datetime
from tqdm import tqdm

target_table='uma_rating_02_form1'

class IDFilter:
    @classmethod
    def generate_phrase(cls, id):
        return " year='%s' AND monthday='%s' AND jyocd='%s' AND kaiji='%s' AND nichiji='%s' AND racenum='%s'" % id

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

class InsertPhrase:
    @classmethod
    def generate(self, id, kettonum, rating):
        print("INSERT INTO %s VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s);" % ((target_table,) + id + (kettonum,) + rating))
        return "INSERT INTO %s VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s);" % ((target_table,) + id + (kettonum,) + rating)

class IDListReference:
    def __init__(self, fromymd, toymd):
        self.table      = 'n_race'
        self.cols       = 'year, monthday, jyocd, kaiji, nichiji, racenum'
        self.conditions = "datakubun='7' AND concat(year, monthday) >= '%s' AND concat(year, monthday) <= '%s'" % (fromymd, toymd)
        self.order      = 'year ASC, monthday ASC, jyocd ASC, nichiji ASC, racenum ASC'
        self.limit      = ''
        #self.limit      = '200'

class HorseInfoReference:
    __cols = 'umaban, kettonum, sexcd, kisyucode, futan, bataijyu, zogenfugo, zogensa, ijyocd, kakuteijyuni'

    def __init__(self, id):
        self.table      = 'n_uma_race'
        self.cols       = HorseInfoReference.__cols
        self.conditions = IDFilter.generate_phrase(id) + " AND datakubun='7'"
        self.order      = 'kettonum DESC'
        self.limit      = ''

    @classmethod
    def index(self, colname):
        return self.__cols.strip().split(', ').index(colname)

class RatingReference:
    __cols = 'year, monthday, jyocd, kaiji, nichiji, racenum, rating'

    def __init__(self, year, monthday, kettonum):
        self.table      = 'uma_rating_02'
        self.cols       = RatingReference.__cols
        self.conditions = "kettonum='%s' and concat(year, monthday) < '%s%s'" % (kettonum, year, monthday)
        self.order      = 'year DESC, monthday DESC'
        self.limit      = '1'

    @classmethod
    def index(self, colname):
        return self.__cols.strip().split(', ').index(colname)

class IDReader:
    @classmethod
    def load_data(self, fromyear, toyear, connection):
        with connection.cursor('id_cursor') as cur:
            query = SelectPhrase.generate(IDListReference(fromyear, toyear))
            cur.execute(query)
            id_list  = cur.fetchall()

        return id_list

class UmaReader:
    @classmethod
    def __get_kettonum_list(self, rows):
        kettonum_list = list()

        for row in rows:
            if row[HorseInfoReference.index('ijyocd')] != '0':
                continue
            kettonum_list.append( row[HorseInfoReference.index('kettonum')] )

        return kettonum_list

    @classmethod
    def load_data(self, id, connection):
        with connection.cursor('uma_cursor') as cur:
            # Get race specific uma info from n_race_uma
            query = SelectPhrase.generate(HorseInfoReference(id))
            cur.execute(query)
            horse_info_list = cur.fetchall()

        kettonum_list = UmaReader.__get_kettonum_list(horse_info_list)

        return kettonum_list

class RatingReader:
    @classmethod
    def load_data(self, id, kettonum_list, connection):
        rating_list = list()

        for kettonum in kettonum_list:
            with connection.cursor('rating_cursor') as cur:
                query = SelectPhrase.generate(RatingReference(id[0], id[1], kettonum))
                cur.execute(query)
                rows = cur.fetchall()

            if rows == None:
                rating_list.append(('0000', '0000', '00', '00', '00', '00', '1400'))
                continue

            if len(rows) == 0:
                rating_list.append(('0000', '0000', '00', '00', '00', '00', '1400'))
                continue

            rating_list.append(rows[0])

        return rating_list

class RatingWriter:
    @classmethod
    def write_data(self, id, kettonum_list, rating_list, connection):
        for kettonum, rating in zip(kettonum_list, rating_list):
            with connection.cursor() as cur:
                query = InsertPhrase.generate(id, kettonum, rating)
                cur.execute(query)
            connection.commit()

class Coupler:
    def __init__(self):
        try:
            self.connection_raw  = psycopg2.connect(os.environ.get('DATABASE_URL_SRC'))
        except:
            print('psycopg2: opening connection 01 faied')
            sys.exit(0)

        try:
            self.connection_processed = psycopg2.connect(os.environ.get('DATABASE_URL_DST'))
        except:
            print('psycopg2: opening connection 02 faied')
            sys.exit(0)

    def __del__(self):
        self.connection_raw.close()
        self.connection_processed.close()

    def process(self, fromyearmonthday, toyearmonthday):
        id_list = IDReader.load_data(fromyearmonthday, toyearmonthday, self.connection_raw)

        kettonum_list = list()
        estimate_current_rating = list()

        for id in tqdm(id_list, desc='Gathering race data'):

            print('\nprocessing id: %s%s%s%s%s%s' % id)

            try:
                kettonum_list = UmaReader.load_data(id, self.connection_raw )
                rating_list = RatingReader.load_data(id, kettonum_list, self.connection_processed)

                RatingWriter.write_data(id, kettonum_list, rating_list, self.connection_processed)

            except RuntimeError as e:
                print(e)
                continue

        print('\n\n\n')

if __name__ == "__main__":
    Coupler().process('19900000', '20200000')

