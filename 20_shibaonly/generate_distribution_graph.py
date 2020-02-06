#!/usr/bin/env python3

import numpy as np
import matplotlib.pyplot as plt
import sys
import os
import psycopg2
import datetime
from tqdm import tqdm

if __name__ == "__main__":

    if (len(sys.argv) != 3):
        print('usage: show_ranking.py <tablename> <yyyymmdd>')

    tablename = sys.argv[1]
    yyyymmdd = sys.argv[2]

    fromyear = str(int(yyyymmdd[:4]) - 1)
    fromdate = yyyymmdd[4:]

    try:
        connection_raw  = psycopg2.connect(os.environ.get('DATABASE_URL_SRC'))
    except:
        print('psycopg2: opening connection 01 faied')
        sys.exit(0)

    try:
        connection_processed = psycopg2.connect(os.environ.get('DB_UMA_PROCESSED'))
    except:
        print('psycopg2: opening connection faied')
        sys.exit(0)

    query_ketto_oneyear = "select kettonum from n_uma_race "\
                      "where (year<'%s' OR (year='%s' AND monthday<'%s')) "\
                      "AND (year>'%d' OR (year='%d' AND monthday>'%s')) "\
                      "order by year desc, monthday desc"\
                      % (yyyymmdd[:4], yyyymmdd[:4], yyyymmdd[4:], int(yyyymmdd[:4]) - 1, int(yyyymmdd[:4]) - 1, yyyymmdd[4:])
    query_birthday= lambda kettonum : "select kettonum, birthdate from n_uma where kettonum='%s'" % (kettonum,)
    query_rating = lambda kettonum : "select year, monthday, rating from %s "\
               "where kettonum='%s' AND (year<'%s' OR (year='%s' AND monthday<'%s'))"\
               "order by year desc, monthday desc "\
               % (tablename, kettonum, yyyymmdd[:4], yyyymmdd[:4], yyyymmdd[4:])

    with connection_raw.cursor() as cur:
        print(query_ketto_oneyear)
        cur.execute(query_ketto_oneyear)
        rows = cur.fetchall()

        kettonum_list = list()
        for row, in rows:
            kettonum_list.append(row)
        kettonum_list_nodup = list(set(kettonum_list))

        print(len(kettonum_list)) 
        print(len(kettonum_list_nodup)) 

    kettonum_birthdate_list = list()
    for kettonum in tqdm(kettonum_list_nodup):
        query = query_birthday(kettonum)
        with connection_raw.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        if rows:
            kettonum_birthdate_list.append((kettonum, rows[0][0]))

    ratings = list()
    for kettonum, birthdate in tqdm(kettonum_birthdate_list):
        query = query_rating(kettonum)
        with connection_processed.cursor('rating') as cur:
            cur.execute(query)
            rating_list = cur.fetchall()

        if rating_list:
            ratings.append(rating_list[0][2])

    connection_processed.close()
    connection_raw.close()

    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)

    ax.hist(ratings, bins=200)
    ax.set_title('Rating appearance in the past year at %s' % yyyymmdd)
    ax.set_xlabel('rating')
    ax.set_ylabel('freq')
    fig.show()
    fig.savefig("%s.png" % yyyymmdd)

