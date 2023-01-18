#!/bin/env python
"""
Backlog loader
Check a given month for any crashes in SODA which are missing from CARTO, and load them into CARTO.

Usage: python check_backlog.py YYYY-MM
Example: python check_backlog.py 2016-07
"""

#
# imports
#

import os
import re
import sys
import datetime
from dateutil.relativedelta import relativedelta
import requests


#
# constants and defines
#

CARTO_USER_NAME = 'chekpeds'
CARTO_API_KEY = os.environ['CARTO_API_KEY'] # make sure this is available in bash as $CARTO_API_KEY

CARTO_CRASHES_TABLE = 'crashes_all_prod'
CARTO_SQL_API_BASEURL = 'https://%s.carto.com/api/v2/sql' % CARTO_USER_NAME
SODA_API_COLLISIONS_BASEURL = 'https://data.cityofnewyork.us/resource/qiz3-axqb.json'

INSERT_CHUNK_SIZE = 40  # inserting into CARTO, do this many records at a time, API time limit


#
# functions
#

def yyyymm2daterange(yyyymm):
    # given our yyyymm parameter, create start & end dates for queries
    # these are INCLUSIVE e.g. January 2015 is 2015-01-01 *through* 2015-01-31
    starting = datetime.date(int(yyyymm[0:4]), int(yyyymm[-2:]), 1)
    ending = starting + relativedelta(months=1)

    starting = starting.isoformat()
    ending = ending.isoformat()

    return (starting, ending)


def getcartoalreadyids(startdate, enddate):
    try:
        sql = "SELECT DISTINCT socrata_id FROM {0} WHERE socrata_id IS NOT NULL AND date_val >= '{1}' AND date_val < '{2}'".format(CARTO_CRASHES_TABLE, startdate, enddate)
        alreadydata = requests.get(
            CARTO_SQL_API_BASEURL,
            params={
                'q': sql,
            }
        ).json()
    except requests.exceptions.RequestException as e:
        print(e.message)
        sys.exit(2)

    if not 'rows' in alreadydata or not len(alreadydata['rows']):
        logger.error('No socrata_id rows: {0}'.format(json.dumps(alreadydata)))
        sys.exit(1)

    # explicitly cast the numeric strings to integers
    socrata_already = [ int(r['socrata_id']) for r in alreadydata['rows'] ]
    return socrata_already


def getsodacrashes(startdate, enddate):
    try:
        whereclause = "crash_date >= '{0}' AND crash_date < '{1}'".format(startdate, enddate)
        crashdata = requests.get(
            SODA_API_COLLISIONS_BASEURL,
            params={
                '$where': whereclause,
                '$order': 'crash_date DESC',
                '$limit': '50000'
            },
            verify=False  # requests hates the SSL certificate due to hostname mismatch, but it IS valid
        ).json()
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


def filtertomissingcrashes(allcrashes, idsalready):
    filtered = []
    for crash in allcrashes:
        if int(crash['collision_id']) not in idsalready:
            filtered.append(crash)
    return filtered


def soda2data(datarows):
    """
    Transforms the JSON SODA response into rows for the SQL insert query
    @param {list} data
    """
    # array to store insert value strings
    vals = []

    # create our value template string once then copy it using string.slice when iterating over data list
    insert_val_template_string = format_string_for_insert_val()

    # iterate over data array and format each dictionary's values into strings for the INSERT SQL query
    for row in datarows:
        datestring = "%sT%s" % (row['crash_date'].split('T')[0], row['crash_time'])
        date_time = datetime.datetime.strptime(datestring, '%Y-%m-%dT%H:%M')

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
        # ditto for 5 potential vehicle_type values
        contributing_factor = format_string_for_postgres_array(row, 'contributing_factor_vehicle')
        vehicle_type = format_string_for_postgres_array(row, 'vehicle_type_code')

        # copy our template value string
        val_string = insert_val_template_string[:]

        # a few rare records lack number_of_persons_X fields, which is a fatal error if we let it go
        if 'number_of_persons_killed' not in row:
            row['number_of_persons_killed'] = int(row['number_of_motorist_killed']) + int(row['number_of_cyclist_killed']) + int(row['number_of_pedestrians_killed'])
        if 'number_of_persons_injured' not in row:
            row['number_of_persons_injured'] = int(row['number_of_motorist_injured']) + int(row['number_of_cyclist_injured']) + int(row['number_of_pedestrians_injured'])

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
            str(row['collision_id'])
        ))

    # ready; return the list of massaged SQL strings
    return vals


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
        print('format_postgres_array must take a valid field name type, %s provided' % field_name)
        sys.exit(2)

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
    # print('SQL UPSERT query:\n %s' % sql)

    return sql


def performcartoquery(query):
    """
    Takes an SQL query and uses it with a POST request to
    the CARTO SQL API. Passing the API key allows for doing
    INSERT, UPDATE, and DELETE queries.
    @param {query} string
    """
    payload = {'q': query, 'api_key': CARTO_API_KEY}

    try:
        r = requests.post(CARTO_SQL_API_BASEURL, data=payload)
        print(r.text)
    except requests.exceptions.RequestException as e:
        print(e.message)
        sys.exit(2)


# https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
def list_chunks(lst, n):
    return [lst[i:i + n] for i in range(0, len(lst), n)]


#
# start execution
#

if __name__ == '__main__':
    if not CARTO_API_KEY:
        print("No CARTO_API_KEY defined in environment")
        sys.exit(1)

    # this validation of the one and only param is somewhat hardcoded to accept only 2015 thru 2018
    # hopefully this is a one-off script
    try:
        yyyymm = sys.argv[1]
        if not re.match(r'^(2015|2016|2017|2018|2019|2020|2021|2022)\-(01|02|03|04|05|06|07|08|09|10|11|12)$', yyyymm):
            raise IndexError
    except IndexError:
        print("Supply a YYYY-MM month. See docs for details.")
        sys.exit(1)

    # get our start & ending dates
    (startdate, enddate) = yyyymm2daterange(yyyymm)
    print("Date range: >= {} AND < {}".format(startdate, enddate))

    # get the list of crash IDs already present at CARTO
    print('Getting SODA IDs for crashes already in CARTO')
    alreadyhaveids = getcartoalreadyids(startdate, enddate)
    print('Got {0} socrata_id entries for existing CARTO records'.format(len(alreadyhaveids)))

    # get the SODA data for this month
    # filter the crashes, to only those we don't already have
    crashesfromsoda = getsodacrashes(startdate, enddate)
    print('Got {0} crashes from SODA entries'.format(len(crashesfromsoda)))
    crashesfromsoda = filtertomissingcrashes(crashesfromsoda, alreadyhaveids)
    print('Filtered to {0} crashes not yet present in CARTO'.format(len(crashesfromsoda)))

    # massage the data
    print('Formatting data...')
    soda2data = soda2data(crashesfromsoda)

    # loop over the insertions in chunks, create SQL, and run it
    done = 0
    insert_chunks = list_chunks(soda2data, INSERT_CHUNK_SIZE)
    for chunk in insert_chunks:
        done += 1
        print("Inserting chunk {} of {}".format(done, len(insert_chunks)))
        insertsql = create_sql_insert(chunk)
        performcartoquery(insertsql)

    # done
    print("Done")
