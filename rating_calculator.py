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

class IDListReference:
    def __init__(self, fromyear, toyear):
        self.table      = 'n_race'
        self.cols       = 'year, monthday, jyocd, kaiji, nichiji, racenum'
        self.conditions = "datakubun='7'" + ' AND' + DateFilter.generate_condition_older(fromyear) + ' AND' + DateFilter.generate_condition_newer(toyear)
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

class StaticalHorseInfoReference:
    def __init__(self, kettonum):
        self.table      = 'n_uma'
        self.cols       = 'birthdate, hinsyucd'
        self.conditions = 'kettonum=' + "'" + kettonum + "'"
        self.order      = ''
        self.limit      = ''

class RaceInfoReference:
    __cols = 'kyori, trackcd, tenkocd, sibababacd, dirtbabacd, syussotosu'
    def __init__(self, id):
        self.table      = 'n_race'
        self.cols       = RaceInfoReference.__cols
        self.conditions = IDFilter.generate_phrase(id) + " AND datakubun='7'"
        self.order      = ''
        self.limit      = ''

    @classmethod
    def index(self, colname):
        return self.__cols.strip().split(', ').index(colname)

class Converter:
    @classmethod
    def raceid_to_datetime(cls, raceid):
        return datetime.date(int(raceid[0]), int(raceid[1][:2]), int(raceid[1][2:4]))

    @classmethod
    def birthdate_to_datetime(cls, birthdate):
        return datetime.date(int(birthdate[:4]), int(birthdate[4:6]), int(birthdate[6:8]))

class RaceInfoLoader:
    @classmethod
    def load_data(self, id, connection):
        with connection.cursor() as cur:
            query = SelectPhrase.generate(RaceInfoReference(id))
            cur.execute(query)
            rows = cur.fetchall()

        if rows == None:
            raise RuntimeError("Unexpected data shortage of raceinfo")

        elif len(rows) > 1:
            print(query, rows)
            raise RuntimeError("Unexpected data duplication in database")

        row = rows[0]

        kyori       = int(row[RaceInfoReference.index('kyori')])

        # 00: Nodata, 10~22: turf, 23~29 dirt, 51~59 Steeple
        trackcd     = int(row[RaceInfoReference.index('trackcd')])
        if trackcd == 0:
            raise RuntimeError("Unexpected data shortage of trackcd")
        elif trackcd >= 10 and trackcd <= 22:
            turf = 1
            dirt = 0
            steeple = 0
        elif trackcd >= 23 and trackcd <= 29:
            turf = 0
            dirt = 1
            steeple = 0
        elif trackcd >= 51 and trackcd <= 59:
            turf = 0
            dirt = 0
            steeple = 1
        else:
            raise NotImplementedError("trackcd: " + tenkocd + " is not implemented")

        # 0: No data, 1: Sunny, 2: Cloudy, 3: Rain, 4: Light rain, 5: Snow, 6: Light Snow
        tenkocd     = int(row[RaceInfoReference.index('tenkocd')])
        # no data or snow is out of scope
        tenko = 0
        if tenkocd == 0:
            raise RuntimeError("Unexpected data shortage of tenkocd")
        if tenkocd >= 5:
            raise RuntimeError("Snow race is not supported")
        elif tenkocd == 1 or tenkocd ==2:
            tenko = 0
        elif tenkocd == 4:
            tenko = 0.5
        elif tenkocd == 3:
            tenko = 1.0

        # 0: No data, 1: Firm, 2: Good, 3: Yielding, 4: Soft
        sibababacd  = int(row[RaceInfoReference.index('sibababacd')])
        dirtbabacd  = int(row[RaceInfoReference.index('dirtbabacd')])

        if sibababacd != 0:
            baba = (sibababacd - 1) / 3.0
        elif dirtbabacd != 0:
            baba = (dirtbabacd - 1) / 3.0
        else:
            raise RuntimeError("Unexpected data shortage of baba")

        syussotosu  = int(row[RaceInfoReference.index('syussotosu')])

        return [kyori, turf, dirt, steeple, tenko, baba, syussotosu]

class UmaInfoLoader:
    @classmethod
    def load_data(self, id, connection):
        with connection.cursor() as cur:
            # Get race specific uma info from n_race_uma
            query = SelectPhrase.generate(HorseInfoReference(id))
            cur.execute(query)

            uma_list = []
            kettonum_list = []
            datapack = []
            first_place = []

            while True:
                row = cur.fetchone()
                if row == None:
                    break
                if row[HorseInfoReference.index('ijyocd')] != '0':
                    continue

                kettonum_list.append( row[HorseInfoReference.index('kettonum')] )
                uma_list.append( row )

            # Get constant uma info from n_uma
            racedate = Converter.raceid_to_datetime(id)

            for (uma, kettonum) in zip(uma_list, kettonum_list):
                query = SelectPhrase.generate(StaticalHorseInfoReference(kettonum))
                cur.execute(query)
                rows = cur.fetchall()
                if rows == None:
                    raise RuntimeError("Unexpected data shortage of statical horse info")

                elif len(rows) > 1:
                    print(query)
                    print(rows)
                    raise RuntimeError("Unexpected data duplication in database")

                row = rows[0]

                birthdate = Converter.birthdate_to_datetime(row[0])

                # Convert raw data for learning
                umaban = int(uma[HorseInfoReference.index('umaban')] )
                sexcd = int(uma[HorseInfoReference.index('sexcd')])
                #kisyucode = int(uma[HorseInfoReference.index('kisyucode')])
                futan = int(uma[HorseInfoReference.index('futan')])
                bataijyu = int(uma[HorseInfoReference.index('bataijyu')])

                kakuteijyuni = int(uma[HorseInfoReference.index('kakuteijyuni')])
                if(kakuteijyuni == 1):
                    first_place.append(1)
                else:
                    first_place.append(0)

                # zogensa is zero when first race 
                zogensa = 0
                if (uma[HorseInfoReference.index('zogenfugo')] + uma[HorseInfoReference.index('zogensa')]):
                    zogensa = int(uma[HorseInfoReference.index('zogenfugo')] + uma[HorseInfoReference.index('zogensa')])
                # [year]
                liveyear = (racedate - birthdate).days / 365

                datapack.append([umaban, sexcd, futan, bataijyu, zogensa, liveyear, 1])

            # padding data
            if len(datapack) > 18:
                raise RuntimeError("Syussotosu is too much")
            for i in range(18 - len(datapack)):
                datapack.append([     0,     0,     0,        0,       0,        0, 0])
                first_place.append(0)

        return datapack, first_place

class psqlproxy:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(os.environ.get('DATABASE_URL'))
        except:
            print('psycopg2 connection faied')

    def __del__(self):
        self.connection.close()

    def __get_race_list_period(self, fromyear, toyear):
        with self.connection.cursor() as cur:
            query = SelectPhrase.generate(IDListReference(fromyear, toyear))
            cur.execute(query)
            rows = cur.fetchall()

        return rows

    def load_data(self, fromyear, toyear):
        id_list = self.__get_race_list_period(fromyear, toyear)

        race_info_list = list()
        uma_info_list = [list() for i in range(18)]
        first_place_list = list()

        for id in tqdm(id_list, desc='Gathering race data'):
            try:
                uma_info, first_place= UmaInfoLoader.load_data(id, self.connection)
                race_info = RaceInfoLoader.load_data(id, self.connection)

            except RuntimeError as e:
                print(e)
                continue

            race_info_list.append(race_info)
            for i in range(18):
                uma_info_list[i].append(uma_info[i])
            first_place_list.append(first_place)
        return np.array([race_info_list,] + uma_info_list), np.array(first_place_list)

if __name__ == "__main__":
    pspr = psqlproxy()
    race, uma, first_place = pspr.load_data('1992', '1993')
    print(uma)
    print(race)
    print(first_place)

