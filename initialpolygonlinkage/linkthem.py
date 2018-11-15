#!/bin/env python
# -*- coding: utf-8 -*-

import requests, os, sys, time

# the table of new polygons, and what field in crashes is used to relate to them
POLYGONS_TABLE = "nyc_assembly"
CRASH_FIELD = "assembly"

# we don't update all crashes fitting into a polygon at once; to be quick enough, we do them in chunks
# how many "chunks" should be used for each polygon?
# lower = more crashes per query, more time processing and less waiting, but likely timeouts
HOWMANY_CHUNKS = 20

# in between each UPDATE query, how many seconds do we sleep?
# we do have API limits, and also don't want to be rude since we are running hundreds of queries
DELAY_BETWEEN_QUERIES = 2

# CARTO constants and configs
CARTO_USER_NAME = 'chekpeds'
CARTO_API_KEY = os.environ['CARTO_API_KEY'] # make sure this is available in bash as $CARTO_API_KEY
CARTO_SQL_API_BASEURL = 'https://%s.carto.com/api/v2/sql' % CARTO_USER_NAME
CRASHES_TABLE = 'crashes_all_prod'



def main():
    # print a summary
    print("This script will populate the {} field from the {} table".format(CRASH_FIELD, POLYGONS_TABLE))
    print("")
    print("If that's right, wait 5 seconds.")
    print("If that's wrong, hit Ctrl-C now to cancel.")
    time.sleep(5)

    print("")

    # query the crashes table and find how many records need updating
    sql = "SELECT COUNT(*) FROM {} WHERE {} IS NULL".format(CRASHES_TABLE, CRASH_FIELD)
    rows = cartoapi_read(sql)
    howmany_crashes = rows[0]['count']
    print("Found {} rows where {} is null".format(howmany_crashes, CRASH_FIELD))

    if not rows[0]['count']:
        print('Looks like all rows are populated, so we have nothing to do. Great!')
        sys.exit(0)

    # query the polygons table and find out how any polygons there are
    sql = "SELECT COUNT(*) FROM {}".format(POLYGONS_TABLE)
    rows = cartoapi_read(sql)
    howmany_polygons = rows[0]['count']
    print("Found {} polygon entries".format(howmany_polygons))
    if not howmany_polygons:
        print('No polygons? That does not look right. Check that.')
        sys.exit(1)

    # quick sanity check; identifier needs to be unique
    # simple check: the same number of distinct identifiers as there are records
    sql = "SELECT DISTINCT identifier FROM {} ORDER BY identifier".format(POLYGONS_TABLE)
    rows = cartoapi_read(sql)
    identifiers_list = [ i['identifier'] for i in rows ]
    howmany_identifiers = len(identifiers_list)
    print("Found {} distinct identifiers for those polygons".format(howmany_identifiers))
    if howmany_identifiers != howmany_polygons:
        print('Whoa there! fewer identifiers than polygons, means they are not unique. Check that.')
        sys.exit(1)

    # generate the list of SQL queries: one polygon/district at a time, crashes divided into X blocks
    update_queries_list = []
    for identifier in identifiers_list:
        for i in range(0, HOWMANY_CHUNKS):
            sql = "UPDATE {crashtable} SET {linkfield}={id} WHERE {linkfield} IS NULL AND ST_INTERSECTS(the_geom, (SELECT the_geom FROM {polytable} WHERE identifier={id})) AND cartodb_id % {chunksize} = {thischunk}".format(
                crashtable=CRASHES_TABLE,
                polytable=POLYGONS_TABLE,
                linkfield=CRASH_FIELD,
                id=identifier,
                chunksize=HOWMANY_CHUNKS,
                thischunk=i
            )
            update_queries_list.append(sql)

    print("")

    # get confirmation; are you sure?
    print("{} polygons X {} chunks = {} queries".format(
        len(identifiers_list),
        HOWMANY_CHUNKS,
        len(update_queries_list)
    ))
    print("")
    print("To continue, just wait 10 seconds.")
    print("To cancel, hit Ctrl-C now.")
    time.sleep(10)

    # start performing them in a loop, with a rest in between them
    done = 0
    for sql in update_queries_list:
        print("[{}/{}] {}".format(done + 1, len(update_queries_list), sql))
        reply = cartoapi_write(sql)
        print("    {} rows done in {} seconds".format(reply['total_rows'], reply['time']))
        done += 1
        time.sleep(DELAY_BETWEEN_QUERIES)
    print("DONE")


def cartoapi_read(sql):
    payload = {'q': sql}

    try:
        data = requests.get(CARTO_SQL_API_BASEURL, params=payload).json()
    except requests.exceptions.RequestException as e:
        print(e.message)
        sys.exit(1)

    if 'rows' not in data:
        print('cartoapi_read() error:\n{}\n{}'.format(sql, json.dumps(data)))
        sys.exit(1)

    return data['rows']


def cartoapi_write(sql):
    payload = {'q': sql, 'api_key': CARTO_API_KEY}

    try:
        reply = requests.post(CARTO_SQL_API_BASEURL, data=payload).json()

        if 'total_rows' not in reply:
            print("ERROR:")
            print(reply)
            sys.exit(1)

        return reply
    except requests.exceptions.RequestException as e:
        print(e.message)
        sys.exit(1)


if __name__ == '__main__':
    if not CARTO_API_KEY:
        print("No CARTO_API_KEY defined in environment")
        sys.exit(1)
    main()
