#!/usr/bin/env python3

import numpy as np
import os
import psycopg2
import datetime
from tqdm import tqdm

class IDFilter:
    @classmethod
    def generate_phrase(cls, id):
        return " year='%s' AND monthday='%s' AND jyocd='%s' AND kaiji='%s' AND nichiji='%s' AND racenum='%s'" % id

class IDFilterUntilToday:
    @classmethod
    def generate_phrase(cls, id):
        return " year<='%s' AND monthday<'%s'" % (id[0], id[1])

class DateFilter:
    @classmethod
    def generate_condition_older(cls, year):
        return " year>='%s'" % year[0:4]

    @classmethod
    def generate_condition_newer(cls, year):
        return " year<='%s'" % year[0:4]

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
        return "INSERT INTO uma_rating_01 VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', %.1f);" % (id + (kettonum,) + (rating,))

class IDListReference:
    def __init__(self, fromyear, toyear):
        self.table      = 'n_race'
        self.cols       = 'year, monthday, jyocd, kaiji, nichiji, racenum'
        self.conditions = "datakubun='7'" + ' AND' + DateFilter.generate_condition_older(fromyear) + ' AND' + DateFilter.generate_condition_newer(toyear)
        self.order      = 'year ASC, monthday ASC, jyocd ASC, nichiji ASC, racenum ASC'
        self.limit      = ''
        #self.limit      = '3'

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

    def __init__(self, id, kettonum):
        self.table      = 'uma_rating_01'
        self.cols       = RatingReference.__cols
        self.conditions = "kettonum='%s'" % kettonum + ' AND' + IDFilterUntilToday.generate_phrase(id)
        self.order      = 'year DESC, monthday DESC'
        self.limit      = '5'

    @classmethod
    def index(self, colname):
        return self.__cols.strip().split(', ').index(colname)

class IDReader:
    @classmethod
    def load_data(self, fromyear, toyear, connection):
        with connection.cursor() as cur:
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
    def __get_kakuteijyuni_list(self, rows):
        kakuteijyuni_list = list()

        for row in rows:
            kakuteijyuni_list.append( row[HorseInfoReference.index('kakuteijyuni')] )

        return kakuteijyuni_list

    @classmethod
    def load_data(self, id, connection):
        with connection.cursor() as cur:
            # Get race specific uma info from n_race_uma
            query = SelectPhrase.generate(HorseInfoReference(id))
            cur.execute(query)
            horse_info_list = cur.fetchall()

            kettonum_list = UmaReader.__get_kettonum_list(horse_info_list)
            kakuteijyuni_list = UmaReader.__get_kakuteijyuni_list(horse_info_list)

        return kettonum_list, kakuteijyuni_list

class RatingReader:
    @classmethod
    def __estimate_current_rating(self, rating_history):
        if not rating_history:
            return 1500
        return rating_history[0][RatingReference.index('rating')]

    @classmethod
    def load_data(self, id, kettonum_list, connection):
        rating_list = list()

        with connection.cursor() as cur:
            for kettonum in kettonum_list:
                query = SelectPhrase.generate(RatingReference(id, kettonum))
                cur.execute(query)
                rating_history = cur.fetchall()

                rating = RatingReader.__estimate_current_rating(rating_history)
                rating_list.append(rating)

        return rating_list

class RatingWriter:
    @classmethod
    def write_data(self, id, kettonum_list, rating_list, connection):
        with connection.cursor() as cur:
            for kettonum, rating in zip(kettonum_list, rating_list):
                query = InsertPhrase.generate(id, kettonum, rating)
                cur.execute(query)

class RatingCalculator:
    k_factor = 32

    @classmethod
    def estimate(self, rating_list, kakuteijyuni_list):
        new_rating_list = list()

        for rating, jyuni in zip(rating_list, kakuteijyuni_list):
            actual_sum = 0
            expect_sum = 0

            for rating_op, jyuni_op in zip(rating_list, kakuteijyuni_list):
                if jyuni == jyuni_op:
                    continue

                actual_sum += 0.0 if jyuni < jyuni_op else 1.0
                expect_sum += 1.0 / (1 + pow(10, (rating_op - rating) / 400.0 ))

            match_num = len(rating_list) - 1
            new_rating = rating + self.k_factor * (actual_sum - expect_sum) / match_num

            new_rating_list.append(new_rating)

        return new_rating_list

class RecordKeeper:
    def __init__(self, comp_func):
        self.record = 1500
        self.kettonum = ''
        self.comp_func_ = comp_func
        self.changed_ = False

    def update(self, kettonum, score):
        if self.comp_func_(score, self.record):
            self.record = score
            self.kettonum = kettonum
            self.changed_ = True

        return self.kettonum, self.record

    def changed(self):
        c = self.changed_
        self.changed_ = False
        return c

class RatingUpdator:
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
        self.connection_raw .close()
        self.connection_processed.close()

    def process(self, fromyear, toyear):
        id_list = IDReader.load_data(fromyear, toyear, self.connection_raw)

        kettonum_list = list()
        estimate_current_rating = list()

        record_min = RecordKeeper( lambda x, record_value: x < record_value )
        record_max = RecordKeeper( lambda x, record_value: x > record_value )

        for id in tqdm(id_list, desc='Gathering race data'):
            try:
                kettonum_list, kakuteijyuni_list= UmaReader.load_data(id, self.connection_raw )
                rating_list = RatingReader.load_data(id, kettonum_list, self.connection_processed)

                new_rating_list = RatingCalculator.estimate(rating_list, kakuteijyuni_list)
                #RatingWriter.write_data(id, kettonum_list, new_rating_list, self.connection_processed)
                self.connection_processed.commit()

                for rating, kettonum in zip(new_rating_list, kettonum_list):
                    print(kettonum, rating)
                    record_min.update(kettonum, rating)
                    record_max.update(kettonum, rating)

                if record_max.changed():
                    print('max: ', record_max.kettonum, record_max.record)

                if record_min.changed():
                    print('min: ', record_min.kettonum, record_min.record)

            except RuntimeError as e:
                print(e)
                continue

if __name__ == "__main__":
    updator = RatingUpdator()
    updator.process('2000', '2010')

