#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import urllib
from datetime import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
import json
import logging
import sys
import os


CARTO_USER_NAME = 'chekpeds'
CARTO_API_KEY = os.environ['CARTO_API_KEY'] # make sure this is available in bash as $CARTO_API_KEY
CARTO_CRASHES_TABLE = 'crashes_all_prod'
CARTO_INTERSECTIONS_TABLE = 'nyc_intersections'
CARTO_SQL_API_BASEURL = 'https://%s.carto.com/api/v2/sql' % CARTO_USER_NAME
SODA_API_COLLISIONS_BASEURL = 'https://data.cityofnewyork.us/resource/qiz3-axqb.json'

logging.basicConfig(
    level=logging.INFO,
    format=' %(asctime)s - %(levelname)s - %(message)s',
    datefmt='%I:%M:%S %p')
logger = logging.getLogger()


def get_date_monthsago_from_carto(monthsago):
    """
    Find a PostgreSQL date string, representing X months ago
    """
    monthsago = int(monthsago)  # make it an integer or die trying
    query = "SELECT current_date - INTERVAL '{0} months' AS backthen".format(monthsago)

    try:
        r = requests.get(CARTO_SQL_API_BASEURL, params={'q': query})
        data = r.json()
    except requests.exceptions.RequestException as e:
        logger.error(e.message)
        sys.exit(1)

    if ('rows' in data) and len(data['rows']):
        datestring = data['rows'][0]['backthen']
        return datestring
    else:
        logger.error('get_date_monthsago_from_carto(): No rows in response from %s' % CARTO_CRASHES_TABLE, json.dumps(data))
        sys.exit(1)


def get_soda_data():
    """
    Make a GET request to the Socrata SODA API for collision data within the last month.
    Limit is purposefully set high as it defaults to 1000, and we routinely see 500 crashes in a single day.
    Make a call to CARTO to get the list of all socrata_id IDs in this same time period.
    """
    amonthago = (date.today() - relativedelta(months=1))

    logger.info('Getting data from Socrata SODA API as of {0}'.format(amonthago))
    try:
        crashdata = requests.get(
            SODA_API_COLLISIONS_BASEURL,
            params={
                '$where': "date >= '%s'" % amonthago.strftime('%Y-%m-%d'),
                '$order': 'date DESC',
                '$limit': '50000'
            },
            verify=False  # requests hates the SSL certificate due to hostname mismatch, but it IS valid
        ).json()
    except requests.exceptions.RequestException as e:
        logger.error(e.message)
        sys.exit(1)

    if isinstance(crashdata, list) and len(crashdata):  # this is good, the expected condition
        logger.info('Got {0} SODA entries OK'.format(len(crashdata)))
        # print(json.dumps(crashdata))
    elif isinstance(crashdata, dict) and crashdata['error']:  # error in SODA API call
        logger.error(crashdata['message'])
        sys.exit(1)
    else:  # no data?
        logger.info('No data returned from Socrata, exiting.')
        sys.exit()

    logger.info('Getting socrata_id list from CARTO as of {0}'.format(amonthago))
    try:
        alreadydata = requests.get(
            CARTO_SQL_API_BASEURL,
            params={
                'q': "SELECT socrata_id FROM {0} WHERE date_val >= '{1}'".format(CARTO_CRASHES_TABLE, amonthago.strftime('%Y-%m-%dT00:00:00Z')),
            }
        ).json()
    except requests.exceptions.RequestException as e:
        logger.error(e.message)
        sys.exit(1)
    if not 'rows' in alreadydata or not len(alreadydata['rows']):
        logger.error('No socrata_id rows: {0}'.format(json.dumps(alreadydata)))
        sys.exit(1)

    socrata_already = [ r['socrata_id'] for r in alreadydata['rows'] ]
    # print(socrata_already)
    logger.info('Got {0} socrata_id entries for existing CARTO records'.format(len(socrata_already)))

    # all done! hand off for real processing
    format_soda_response(crashdata, socrata_already)



def format_string_for_postgres_array(values, field_name):
    """
    Takes a values dictionary and field_name string as input and
    converts them to a string formatted for a postgres text array.
    @param {values} dictionary
    @param {field_name} string
    Any value for either contributing_factor_vehicle_* or vehicle_type_code*
    from the SODA response are joined into a single string surrounded by {}
    """
    tmp_list = []

    if field_name != 'contributing_factor_vehicle' and field_name != 'vehicle_type_code':
        logger.error('format_postgres_array must take a valid field name type, %s provided' % field_name)
        sys.exit(1)

    # if field name matches contributing_factor_vehicle_{n} or vehicle_type_code{n}
    for i in range(1, 6):
        if field_name == 'contributing_factor_vehicle' or (field_name == 'vehicle_type_code' and i > 2):
            field_name_full = "{0}_{1}".format(field_name, i)
        else:
            field_name_full = "{0}{1}".format(field_name, i)

        if field_name_full in values:
            tmp_list.append("'{0}'".format(values[field_name_full]))

    return "ARRAY[%s]::text[]" % ','.join(tmp_list)


def format_string_for_insert_val():
    """
    Creates a placeholder string like \"({0}, {1}, {2}, {3}, {4}, ...)\" for the
    Postgres INSERT value. Some of the {0} get \"dollar\" quotes for fields in the
    crashes table that are of type text
    """
    val_string_tmp = []

    for i in range(0, 23):
        if i < 8 or i >= 14:
            val_string_tmp.append("{%d}" % i)
        elif i == 13:
            val_string_tmp.append("'{%d}'::timestamptz" % i)
        else:
            val_string_tmp.append("$${%d}$$" % i)

    return '(' + ','.join(val_string_tmp) + ')'


def format_soda_response(datarows, already_ids):
    """
    Transforms the JSON SODA response into rows for the SQL insert query
    @param {list} data
    """
    # logger.info('Processing {} rows from SODA API.'.format(len(datarows)))

    # array to store insert value strings
    vals = []

    # create our value template string once then copy it using string.slice when iterating over data list
    insert_val_template_string = format_string_for_insert_val()

    # iterate over data array and format each dictionary's values into strings for the INSERT SQL query
    for row in datarows:
        # this is already present at CARTO, don't insert a duplicate!
        # see also create_sql_insert() which has a check as well, but it's A LOT more efficient to bail here
        if int(row['unique_key']) in already_ids:
            continue

        datestring = "%sT%s" % (row['date'].split('T')[0], row['time'])
        date_time = datetime.strptime(datestring, '%Y-%m-%dT%H:%M')

        # latitude and longitude may or may not be present
        if 'longitude' in row:
            lng = row['longitude']
        else:
            lng = None

        if 'latitude' in row:
            lat = row['latitude']
        else:
            lat = None

        if lat and lng:
            the_geom = "ST_GeomFromText('Point({0} {1})', 4326)".format(lng, lat)
        else:
            the_geom = 'null'
            lat = 'null'
            lng = 'null'

        # ditto for on_street_name
        # dollar quote strings to escape single quotes in street names like "O'Brien"
        if 'on_street_name' in row:
            on_street_name = row['on_street_name'].strip()
        else:
            on_street_name = ''

        # ditto for off_street_name
        if 'off_street_name' in row:
            off_street_name = row['off_street_name'].strip()
        else:
            off_street_name = ''

        # ditto for cross_street_name
        if 'cross_street_name' in row:
            cross_street_name = row['cross_street_name'].strip()
        else:
            cross_street_name = ''

        # ditto for zip_code
        if 'zip_code' in row:
            zipcode = row['zip_code']
        else:
            zipcode = ''

        # format 5 potential values for contributing_factor into a string formatted for a Postgres array
        contributing_factor = format_string_for_postgres_array(row, 'contributing_factor_vehicle')

        # ditto for 5 potential vehicle_type values
        vehicle_type = format_string_for_postgres_array(row, 'vehicle_type_code')

        # copy our template value string
        val_string = insert_val_template_string[:]

        # create the sql value string
        vals.append(val_string.format(
            row['number_of_motorist_killed'],
            row['number_of_motorist_injured'],
            row['number_of_cyclist_killed'],
            row['number_of_cyclist_injured'],
            row['number_of_pedestrians_killed'],
            row['number_of_pedestrians_injured'],
            row['number_of_persons_killed'],
            row['number_of_persons_injured'],
            zipcode,
            off_street_name,
            cross_street_name,
            on_street_name,
            '',  # leave borough blank, update_borough() does a better job
            date_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            lng,
            lat,
            the_geom,
            vehicle_type,
            contributing_factor,
            date_time.strftime('%Y'),
            date_time.strftime('%m'),
            '1',
            str(row['unique_key'])
        ))

        # print([ str(row['unique_key']), date_time.strftime('%Y-%m-%dT%H:%M:%SZ'),lng, lat ])

    logger.info('Found {0} new rows to insert into CARTO'.format(len(vals)))

    # ready, go ahead and submit them
    update_carto_table(vals)


def create_sql_insert(vals):
    """
    Creates the SQL INSERT statment using a list of formatted strings for
    each row being inserted.
    @param {vals} list of strings
    """
    # field names for the crashes table which get values inserted into them
    column_names_string = '''
    number_of_motorist_killed
    number_of_motorist_injured
    number_of_cyclist_killed
    number_of_cyclist_injured
    number_of_pedestrian_killed
    number_of_pedestrian_injured
    number_of_persons_killed
    number_of_persons_injured
    zip_code
    off_street_name
    cross_street_name
    on_street_name
    borough
    date_val
    longitude
    latitude
    the_geom
    vehicle_type
    contributing_factor
    year
    month
    crash_count
    socrata_id
    '''
    column_name_list_tmp = [n.strip() for n in column_names_string.split('\n')]
    column_name_list = [n for n in column_name_list_tmp if n != '']

    # only insert data that doesn't exist in our table already
    sql = '''
    WITH
    n({0}) AS (
    VALUES {1}
    )
    INSERT INTO {2} ({0})
    SELECT n.number_of_motorist_killed,
    n.number_of_motorist_injured,
    n.number_of_cyclist_killed,
    n.number_of_cyclist_injured,
    n.number_of_pedestrian_killed,
    n.number_of_pedestrian_injured,
    n.number_of_persons_killed,
    n.number_of_persons_injured,
    n.zip_code,
    n.off_street_name,
    n.cross_street_name,
    n.on_street_name,
    n.borough,
    n.date_val,
    n.longitude,
    n.latitude,
    n.the_geom,
    n.vehicle_type,
    n.contributing_factor,
    n.year,
    n.month,
    n.crash_count,
    n.socrata_id
    FROM n
    WHERE n.socrata_id NOT IN (
    SELECT socrata_id FROM {2}
    WHERE socrata_id IS NOT NULL
    )
    '''.format(','.join(column_name_list), ','.join(vals), CARTO_CRASHES_TABLE)
    # logger.info('SQL UPSERT query:\n %s' % sql)

    return sql


def filter_carto_data():
    """
    SQL query that filters out data outside of NYC, including incorrectly geocoded data.
    """

    sql = '''
    UPDATE {0}
    SET the_geom = NULL
    WHERE cartodb_id IN
    (
    WITH box AS (
    SELECT ST_SetSRID(ST_Extent(the_geom), 4326)::geometry as the_geom,
    666 as cartodb_id
    FROM nyc_borough
    )
    SELECT
    c.cartodb_id
    FROM {0} AS c
    LEFT JOIN
    box AS a ON
    ST_Intersects(c.the_geom, a.the_geom)
    WHERE a.cartodb_id IS NULL
    AND c.the_geom IS NOT NULL
    )
    '''.format(CARTO_CRASHES_TABLE)
    # logger.info('SQL UPDATE query:\n %s' % sql)

    return sql

def update_borough():
    """
    SQL query that updates the borough column in the crashes table
    """
    logger.info('Cleanup update_borough()')

    sql = '''
    UPDATE {0}
    SET borough = a.borough
    FROM nyc_borough a
    WHERE {0}.the_geom IS NOT NULL AND ST_Within({0}.the_geom, a.the_geom)
    AND ({0}.borough IS NULL OR {0}.borough='')
    '''.format(CARTO_CRASHES_TABLE)
    return sql

def update_city_council():
    """
    SQL query that updates the city_council column in the crashes table
    """
    logger.info('Cleanup update_city_council()')

    sql = '''
    UPDATE {0}
    SET city_council = a.identifier
    FROM nyc_city_council a
    WHERE {0}.the_geom IS NOT NULL AND ST_Within({0}.the_geom, a.the_geom)
    AND ({0}.city_council IS NULL)
    '''.format(CARTO_CRASHES_TABLE)
    return sql

def update_community_board():
    """
    SQL query that updates the community_board column in the crashes table
    """
    logger.info('Cleanup update_community_board()')

    sql = '''
    UPDATE {0}
    SET community_board = a.identifier
    FROM nyc_community_board a
    WHERE {0}.the_geom IS NOT NULL AND ST_Within({0}.the_geom, a.the_geom)
    AND ({0}.community_board IS NULL)
    '''.format(CARTO_CRASHES_TABLE)
    return sql

def update_neighborhood():
    """
    SQL query that updates the neighborhood column in the crashes table
    """
    logger.info('Cleanup update_neighborhood()')

    sql = '''
    UPDATE {0}
    SET neighborhood = a.identifier
    FROM nyc_neighborhood a
    WHERE {0}.the_geom IS NOT NULL AND ST_Within({0}.the_geom, a.the_geom)
    AND ({0}.neighborhood IS NULL OR {0}.neighborhood='')
    '''.format(CARTO_CRASHES_TABLE)
    return sql

def update_nypd_precinct():
    """
    SQL query that updates the nypd_precinct column in the crashes table
    """
    logger.info('Cleanup update_nypd_precinct()')

    sql = '''
    UPDATE {0}
    SET nypd_precinct = a.identifier::int
    FROM nyc_nypd_precinct a
    WHERE {0}.the_geom IS NOT NULL AND ST_Within({0}.the_geom, a.the_geom)
    AND ({0}.nypd_precinct IS NULL)
    '''.format(CARTO_CRASHES_TABLE)
    return sql

def make_carto_sql_api_request(query):
    """
    Takes an SQL query and uses it with a POST request to
    the CARTO SQL API. Passing the API key allows for doing
    INSERT, UPDATE, and DELETE queries.
    @param {query} string
    """
    payload = {'q': query, 'api_key': CARTO_API_KEY}

    try:
        r = requests.post(CARTO_SQL_API_BASEURL, data=payload)
        logger.info(r.text)
    except requests.exceptions.RequestException as e:
        logger.error(e.message)
        sys.exit(1)


def update_intersections_crashcount():
    """
    Update the nyc_intersections table crashcount field, the number of crashes found within that circle
    in the last M months, which did have at least 1 fatality or injury.
    We can't do this in one query, due to CARTO's 15-second timeout, but we can do X queries to every Xth record, using modulus % operator.
    """

    logger.info('Intersections crashcount reset')
    make_carto_sql_api_request("UPDATE {0} SET crashcount=NULL".format(CARTO_INTERSECTIONS_TABLE))

    sincewhen = get_date_monthsago_from_carto(36)

    howmanyblocks = 5
    for thisblock in range(0, howmanyblocks):
        logger.info('Intersections crashcount dated {2}: {0}/{1}'.format(thisblock+1, howmanyblocks, sincewhen))
        sql = """
        WITH counts AS (
            SELECT {0}.the_geom, {0}.cartodb_id, COUNT(*) AS howmany
            FROM {1}
            JOIN {0} ON
                ST_CONTAINS({0}.the_geom,{1}.the_geom)
                AND {1}.date_val >= '{4}'
                AND ({1}.number_of_persons_injured > 0 OR {1}.number_of_persons_killed > 0)
                AND {0}.cartodb_id % {3} = {2}
            GROUP BY {0}.cartodb_id
        )
        UPDATE {0}
        SET crashcount = counts.howmany
        FROM counts
        WHERE {0}.cartodb_id = counts.cartodb_id
        """.format(CARTO_INTERSECTIONS_TABLE, CARTO_CRASHES_TABLE, thisblock, howmanyblocks, sincewhen)
        make_carto_sql_api_request(sql)



def update_carto_table(vals):
    """
    Updates the master crashes table on CARTO.
    """
    # insert the new data
    make_carto_sql_api_request(create_sql_insert(vals))

def main():
    # get the most recent data from New York's data endpoint, and load it
    get_soda_data()

    # filter out any poorly geocoded data afterward (e.g. null island)
    make_carto_sql_api_request(filter_carto_data())

    # update the borough, city councily, nypd precinct, ...
    make_carto_sql_api_request(update_borough())
    make_carto_sql_api_request(update_city_council())
    make_carto_sql_api_request(update_community_board())
    make_carto_sql_api_request(update_neighborhood())
    make_carto_sql_api_request(update_nypd_precinct())

    # update the nyc_intersections crashcount field
    update_intersections_crashcount()


if __name__ == '__main__':
    if not CARTO_API_KEY:
        print("No CARTO_API_KEY defined in environment")
        sys.exit(1)
    main()
