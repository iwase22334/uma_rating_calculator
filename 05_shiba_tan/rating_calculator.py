#!/usr/bin/env python3

import numpy as np
import os
import psycopg2
import datetime
from tqdm import tqdm

target_table='uma_rating_05'

class IDFilter:
    @classmethod
    def generate_phrase(cls, id):
        return " year='%s' AND monthday='%s' AND jyocd='%s' AND kaiji='%s' AND nichiji='%s' AND racenum='%s'" % id

class IDFilterUntilToday:
    @classmethod
    def generate_phrase(cls, id):
        return " concat(year, monthday)<'%s'" % (id[0] + id[1],)

class DateFilter:
    @classmethod
    def generate_condition_older(cls, yearmonthday):
        return " concat(year, monthday)>='%s'" % yearmonthday

    @classmethod
    def generate_condition_newer(cls, yearmonthday):
        return " concat(year, monthday)<='%s'" % yearmonthday

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
        return "INSERT INTO %s VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', %.1f);" % ((target_table,) + id + (kettonum,) + (rating,))

class IDListReference:
    def __init__(self, fromyearmonthday, toyearmonthday):
        self.table      = 'n_race'
        self.cols       = 'year, monthday, jyocd, kaiji, nichiji, racenum'
        self.conditions = "datakubun='7'" + ' AND' + DateFilter.generate_condition_older(fromyearmonthday) + ' AND' + DateFilter.generate_condition_newer(toyearmonthday)
        self.order      = 'year ASC, monthday ASC, jyocd ASC, nichiji ASC, racenum ASC'
        self.limit      = ''
        #self.limit      = '3'

class RaceInfoReference:
    def __init__(self, id):
        self.table      = 'n_race'
        self.cols       = 'trackcd'
        self.conditions = IDFilter.generate_phrase(id) + " AND datakubun='7'"
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
        self.table      = target_table
        self.cols       = RatingReference.__cols
        self.conditions = "kettonum='%s'" % kettonum + ' AND' + IDFilterUntilToday.generate_phrase(id)
        self.order      = 'year DESC, monthday DESC'
        self.limit      = '3'

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

class TrackcdReader:
    @classmethod
    def load_data(self, id, connection):
        with connection.cursor('uma_cursor') as cur:
            query = "select trackcd from n_race where %s AND datakubun='7'" % (IDFilter.generate_phrase(id),)
            cur.execute(query)
            row = cur.fetchone()

        return row[0]

class UmaReader:

    @classmethod
    def __analyze(self, rows):
        kakuteijyuni_list = list()
        kettonum_list = list()

        for row in rows:
            if row[HorseInfoReference.index('ijyocd')] != '0':
                continue
            kettonum_list.append( row[HorseInfoReference.index('kettonum')] )
            kakuteijyuni_list.append( row[HorseInfoReference.index('kakuteijyuni')] )

        return kettonum_list, kakuteijyuni_list

    @classmethod
    def load_data(self, id, connection):
        with connection.cursor('uma_cursor') as cur:
            # Get race specific uma info from n_race_uma
            query = SelectPhrase.generate(HorseInfoReference(id))
            cur.execute(query)
            horse_info_list = cur.fetchall()

            kettonum_list, kakuteijyuni_list = UmaReader.__analyze(horse_info_list)

        return kettonum_list, kakuteijyuni_list

class RatingReader:
    @classmethod
    def __estimate_current_rating(self, rating_history):
        if not rating_history:
            return 1400
        return rating_history[0][RatingReference.index('rating')]

    @classmethod
    def load_data(self, id, kettonum_list, connection):
        rating_list = list()

        for kettonum in kettonum_list:
            with connection.cursor('rating_cursor') as cur:
                query = SelectPhrase.generate(RatingReference(id, kettonum))
                cur.execute(query)
                rating_history = cur.fetchall()

            rating = RatingReader.__estimate_current_rating(rating_history)
            rating_list.append(rating)

        return rating_list

class RatingWriter:
    @classmethod
    def write_data(self, id, kettonum_list, rating_list, connection):
        for kettonum, rating in zip(kettonum_list, rating_list):
            with connection.cursor() as cur:
                query = InsertPhrase.generate(id, kettonum, rating)
                cur.execute(query)
        connection.commit()

class RatingCalculator:
    k_factor = 32

    @classmethod
    def estimate(self, rating_list, kakuteijyuni_list):
        assert(len(rating_list) == len(kakuteijyuni_list))
        new_rating_list = list()

        rating_top = 0
        for rating, jyuni in zip(rating_list, kakuteijyuni_list):
            if jyuni == '01':
                rating_top = rating
                break

        if rating_top == 0:
            print('invalid data')
            print(rating_list)
            print(kakuteijyuni_list)
            exit()

        for rating, jyuni in zip(rating_list, kakuteijyuni_list):
            actual_sum = 0
            expect_sum = 0

            if jyuni == '01':
                for rating_op, jyuni_op in zip(rating_list, kakuteijyuni_list):
                    if jyuni == jyuni_op:
                        continue
                    expect_sum += 1.0 / (1 + pow(10, (rating_op - rating) / 400.0 ))

                actual_sum = len(rating_list) - 1
                new_rating = rating + self.k_factor * (actual_sum - expect_sum) 

            else:
                expect = 1.0 / (1 + pow(10, (rating_top - rating) / 400.0 ))
                new_rating = rating + self.k_factor * (0 - expect)

            new_rating_list.append(new_rating)

        return new_rating_list

class RecordKeeper:
    def __init__(self, comp_func):
        self.record = 1400
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
            self.connection_processed = psycopg2.connect(os.environ.get('DB_UMA_PROCESSED'))
        except:
            print('psycopg2: opening connection 02 faied')
            sys.exit(0)

    def __del__(self):
        self.connection_raw .close()
        self.connection_processed.close()

    def process(self, fromyearmonthday, toyearmonthday):
        id_list = IDReader.load_data(fromyearmonthday, toyearmonthday, self.connection_raw)

        kettonum_list = list()
        estimate_current_rating = list()

        record_min = RecordKeeper( lambda x, record_value: x < record_value )
        record_max = RecordKeeper( lambda x, record_value: x > record_value )

        count = 0
        for id in tqdm(id_list, desc='Gathering race data'):

            print('\nprocessing id: %s%s%s%s%s%s' % id, ' [%s]' % (count, ))
            print('max: [%s, %.1lf]' % (record_max.kettonum, record_max.record))
            print('min: [%s, %.1lf]' % (record_min.kettonum, record_min.record), end='\033[3A\r', flush=True)

            trackcd = TrackcdReader.load_data(id, self.connection_raw)
            if not trackcd.isdigit():
                print("invalid trackcd:", id, trackcd)

            elif trackcd == "00":
                print("notset:", id,  trackcd)

            elif int(trackcd) <= 22:
                try:
                    kettonum_list, kakuteijyuni_list= UmaReader.load_data(id, self.connection_raw )
                    rating_list = RatingReader.load_data(id, kettonum_list, self.connection_processed)

                    new_rating_list = RatingCalculator.estimate(rating_list, kakuteijyuni_list)
                    RatingWriter.write_data(id, kettonum_list, new_rating_list, self.connection_processed)

                    for rating, kettonum in zip(new_rating_list, kettonum_list):
                        record_min.update(kettonum, rating)
                        record_max.update(kettonum, rating)

                    count = count + 1

                except RuntimeError as e:
                    print(e)
                    continue

        print('\n\n\n\n')

if __name__ == "__main__":
    updator = RatingUpdator()
    updator.process('19900000', '20200000')
    #updator.process('19990000', '20100000')
    #updator.process('20100000', '20200000')

