#!/bin/env python3
"""
Look over CARTO records with blank geometry, see if we have updated latitude & longitude from Socrata.
"""

import requests
import os
import sys
import re
import datetime
from dateutil.relativedelta import relativedelta
from time import sleep
import logging


CARTO_USER_NAME = 'chekpeds'
CARTO_API_KEY = os.environ['CARTO_API_KEY'] # make sure this is available in bash as $CARTO_API_KEY
CARTO_CRASHES_TABLE = 'crashes_all_prod'
CARTO_SQL_API_BASEURL = 'https://%s.carto.com/api/v2/sql' % CARTO_USER_NAME
SODA_API_COLLISIONS_BASEURL = 'https://data.cityofnewyork.us/resource/qiz3-axqb.json'

################################################################################################


logging.basicConfig(
    level=logging.INFO,
    format=' %(asctime)s - %(levelname)s - %(message)s',
    datefmt='%I:%M:%S %p')
logger = logging.getLogger()


def run():
    # get our start & ending dates
    try:
        yyyymm = sys.argv[1]
        if not re.match(r'^(2015|2016|2017|2018|2019|2020|2021)\-(01|02|03|04|05|06|07|08|09|10|11|12)$', yyyymm):
            raise IndexError
    except IndexError:
        print("Supply a YYYY-MM month. See docs for details.")
        sys.exit(1)

    (startdate, enddate) = yyyymm2daterange(yyyymm)
    print("Date range: >= {} AND < {}".format(startdate, enddate))

    print("Checking CARTO for records with null geom")
    null_geom_socrata_ids = list_cartodb_null_geoms(startdate, enddate)
    print("Found {} records with null geom".format(len(null_geom_socrata_ids)))

    print("Checking Socrata")
    newcrashdata = get_soda_for_collision_ids(null_geom_socrata_ids)
    print("Found geom for {} records".format(len(newcrashdata)))

    for crashinfo in newcrashdata:
        update_carto_geom(crashinfo)

    print("DONE")


def yyyymm2daterange(yyyymm):
    # given our yyyymm parameter, create start & end dates for queries
    # these are INCLUSIVE e.g. January 2015 is 2015-01-01 *through* 2015-01-31
    starting = datetime.date(int(yyyymm[0:4]), int(yyyymm[-2:]), 1)
    ending = starting + relativedelta(months=1)

    starting = starting.isoformat()
    ending = ending.isoformat()

    return (starting, ending)


def get_soda_for_collision_ids(collisionids):
    try:
        whereclause = "collision_id IN ({}) AND latitude IS NOT NULL AND latitude != '0.0000000'".format(','.join([str(i) for i in collisionids]))
        crashdata = requests.get(
            SODA_API_COLLISIONS_BASEURL,
            params={
                '$where': whereclause,
                '$order': 'crash_date ASC',
                '$limit': '50000',
            },
            verify=False  # requests hates the SSL certificate due to hostname mismatch, but it IS valid
        ).json()
        return crashdata
    except requests.exceptions.RequestException as e:
        print(e.message)
        sys.exit(1)

    if isinstance(crashdata, list) and len(crashdata):  # this is good, the expected condition
        return crashdata
    elif isinstance(crashdata, dict) and crashdata['error']:  # error in SODA API call
        logger.error(crashdata['message'])
        sys.exit(2)
    else:  # no data?
        print('No data returned from Socrata, exiting.')
        sys.exit(2)


def performcartoquery(query):
    # POST the given SQL query to CARTO
    try:
        r = requests.post(CARTO_SQL_API_BASEURL, data={'q': query, 'api_key': CARTO_API_KEY})
        data = r.json()
    except requests.exceptions.RequestException as e:
        logger.error(e.message)
        sys.exit(1)

    if ('rows' in data) and len(data['rows']):
        return data['rows']
    elif 'error' in data:
        logger.error('performcartoquery(): Failed query\n    {}\n    {}'.format(query,  data['error']))
        sys.exit(1)


def list_cartodb_null_geoms(startdate, enddate):
    sql = """
    SELECT cartodb_id, socrata_id, date_val::date
    FROM crashes_all_prod
    WHERE socrata_id IS NOT NULL AND (the_geom IS NULL OR ST_X(the_geom) = 0)
    AND date_val >= '{}' AND date_val < '{}'
    ORDER BY date_val
    """.format(startdate, enddate)
    rows = performcartoquery(sql)
    ids = [i['socrata_id'] for i in rows]
    return ids


def update_carto_geom(crashinfo):
    socrata_id = crashinfo['collision_id']
    crashdate = crashinfo['crash_date'][:10]
    longitude = float(crashinfo['longitude'])
    latitude = float(crashinfo['latitude'])
    the_geom = "POINT({} {})".format(longitude, latitude)

    print("    socrata_id={}    date={}    latitude={}, longitude={}".format(socrata_id, crashdate, latitude, longitude))
    sql = "UPDATE {} SET latitude={}, longitude={}, the_geom=ST_GEOMFROMTEXT('{}', 4326) WHERE socrata_id={}".format(
        CARTO_CRASHES_TABLE,
        latitude,
        longitude,
        the_geom,
        socrata_id
    )
    performcartoquery(sql)

    sleep(5)


if __name__ == '__main__':
    if not CARTO_API_KEY:
        print("No CARTO_API_KEY defined in environment")
        sys.exit(1)
    run()
