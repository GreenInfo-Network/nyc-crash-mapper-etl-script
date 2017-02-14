#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import urllib
import datetime
import json
import logging
import sys
import os


CARTO_USER_NAME = 'chekpeds'
CARTO_API_KEY = os.environ['CARTO_API_KEY'] # make sure this is available in bash as $CARTO_API_KEY
CARTO_CRASHES_TABLE = 'etl_test'
CARTO_SQL_API_BASEURL = 'https://%s.carto.com/api/v2/sql' % CARTO_USER_NAME
SODA_API_COLLISIONS_BASEURL = 'https://data.cityofnewyork.us/resource/qiz3-axqb.json'

logging.basicConfig(
    level=logging.INFO,
    format=' %(asctime)s - %(levelname)s - %(message)s',
    datefmt='%I:%M:%S %p')
logger = logging.getLogger()


def get_max_date_from_carto():
  """
  Makes a GET request to the CARTO SQL API for the most recent date
  from the crash data table. as a datetime.date object. Uses a datetime.timedelta
  to go back in time by 90 days from the crashes table date. This is because the
  Socrata data may not be updated for a few months at a time. Thus the time delta
  attempts to capture any data recently added.
  """
  query='SELECT max(date_val) as latest_date FROM %s' % CARTO_CRASHES_TABLE
  logger.info('Getting latest date from table %s...' % CARTO_CRASHES_TABLE)

  try:
    r = requests.get(CARTO_SQL_API_BASEURL, params={'q': query})
  except requests.exceptions.RequestException as e:
    logger.error(e.message)
    sys.exit(1)

  data = r.json()

  if ('rows' in data) and len(data['rows']):
    datestring = data['rows'][0]['latest_date']
    latest_date = datetime.datetime.strptime(datestring, '%Y-%m-%dT%H:%M:%SZ')
    logger.info('Latest date from table %s is %s' % (CARTO_CRASHES_TABLE, latest_date))
  else:
    logger.error('No rows in response from %s' % CARTO_CRASHES_TABLE, json.dumps(data))
    sys.exit(1)

  # make sure there's overlap in case the Socrata data hasn't been updated in a few months...
  delta = datetime.timedelta(days=90)
  overlap = latest_date - delta

  return overlap


def get_soda_data(dateobj):
  """
  Makes a GET request to the Socrata SODA API for collision data
  using a `where date > dateobj` filter in the request.
  @param {dateobj} datetime.date object
  """
  datestring = dateobj.strftime('%Y-%m-%d')
  baseurl = "https://data.cityofnewyork.us/resource/qiz3-axqb.json"
  payload = {
    '$where': "date >= '%s'" % datestring,
    '$order': 'date DESC',
    '$limit': '60000'
  }

  logger.info('Getting latest collision data from Socrata SODA API...')

  try:
    r = requests.get(baseurl, params=payload)
  except requests.exceptions.RequestException as e:
    logger.error(e.message)
    sys.exit(1)

  data = r.json()

  if isinstance(data, list) and len(data):
    # there's data!
    format_soda_response(r.json())
  elif isinstance(data, dict) and data['error']:
    # error in SODA API call
    logger.error(data['message'])
    sys.exit(1)
  else:
    logger.info('No data returned from Socrata, exiting.')
    sys.exit()


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

  for i in range(1, 6):
    if field_name == 'contributing_factor_vehicle' or (field_name == 'vehicle_type_code' and i > 2):
      field_name_full = "{0}_{1}".format(field_name, i)
    else: #elif field_name == 'vehicle_type_code' and i < 3:
      field_name_full = "{0}{1}".format(field_name, i)

    if field_name_full in values:
      tmp_list.append(values[field_name_full])

  return "ARRAY['%s']" % ','.join(tmp_list)


def format_string_for_insert_val():
  """
  Creates a placeholder string like \"({0}, {1}, {2}, {3}, {4}, ...)\" for the
  Postgres INSERT value. Some of the {0} get single quotes for fields in the
  crashes table that are of type text
  """
  val_string_tmp = []

  for i in range(0, 23):
    if i < 8 or i >= 14:
      val_string_tmp.append("{%d}" % i)
    elif i == 13:
      val_string_tmp.append("'{%d}'::timestamptz" % i)
    else:
      val_string_tmp.append("'{%d}'" % i)

  return '(' + ','.join(val_string_tmp) + ')'


def format_soda_response(data):
  """
  Transforms the JSON SODA response into rows for the SQL insert query
  @param {list} data
  """
  logger.info('Processing {} rows from SODA API.'.format(len(data)))

  # array to store insert value strings
  vals = []

  # create our insert value template string once, copy it using string.slice when iterating over data list
  insert_val_template_string = format_string_for_insert_val()

  # iterate over data array and format each dictionary's values into strings for the INSERT SQL query
  for row in data:
    datestring = "%sT%s" % (row['date'].split('T')[0], row['time'])
    date_time = datetime.datetime.strptime(datestring, '%Y-%m-%dT%H:%M')

    # the borough key is not always included in SODA response object,
    # use an empty string when it's not present
    if 'borough' in row:
      borough = row['borough']
    else:
      borough = ''

    # ditto for longitude
    if 'longitude' in row:
      lng = row['longitude']
    else:
      lng = None

    # ditto for latitude
    if 'latitude' in row:
      lat = row['latitude']
    else:
      lat = None

    # if we have lat and lng, create a PostGIS geom from text statement
    # else make lat, lng, and geom 'null'
    if lat and lng:
      the_geom = "ST_GeomFromText('Point({0} {1})', 4326)".format(lng, lat)
    else:
      the_geom = 'null'
      lat = 'null'
      lng = 'null'

    # ditto for on_street_name
    if 'on_street_name' in row:
      on_street_name = row['on_street_name']
    else:
      on_street_name = ''

    # ditto for off_street_name
    if 'off_street_name' in row:
      off_street_name = row['off_street_name']
    else:
      off_street_name = ''

    # ditto for cross_street_name
    if 'cross_street_name' in row:
      cross_street_name = row['cross_street_name']
    else:
      cross_street_name = ''

    if 'zip_code' in row:
      zipcode = row['zip_code']
    else:
      zipcode = ''

    # format 5 potential values for contributing_factor into a string formatted for a Postgres array
    contributing_factor = format_string_for_postgres_array(row, 'contributing_factor_vehicle')

    # ditto for 5 potential vehicle_type values
    vehicle_type = format_string_for_postgres_array(row, 'vehicle_type_code')

    val_string = insert_val_template_string[:]

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
      borough,
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

  create_sql_insert(vals)


def create_sql_insert(vals):
  """
  Creates the SQL INSERT statment using a list of strings formatted to match values
  @param {vals} list of strings
  """
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

  # could just change this to an insert where the socrata_id doesn't exist,
  # as there's no need to update rows that already exist
  upsert='''
  WITH
  n({0}) AS (
  VALUES {1}
  ),
  upsert AS (
  UPDATE {2} o
  SET number_of_motorist_killed = n.number_of_motorist_killed,
  number_of_motorist_injured = n.number_of_motorist_injured,
  number_of_cyclist_killed = n.number_of_cyclist_killed,
  number_of_cyclist_injured = n.number_of_cyclist_injured,
  number_of_pedestrian_killed = n.number_of_pedestrian_killed,
  number_of_pedestrian_injured = n.number_of_pedestrian_injured,
  number_of_persons_killed = n.number_of_persons_killed,
  number_of_persons_injured = n.number_of_persons_injured,
  zip_code = n.zip_code,
  off_street_name = n.off_street_name,
  cross_street_name = n.cross_street_name,
  on_street_name = n.on_street_name,
  borough = n.borough,
  date_val = n.date_val,
  longitude = n.longitude,
  latitude = n.latitude,
  the_geom = n.the_geom,
  vehicle_type = n.vehicle_type,
  contributing_factor = n.contributing_factor,
  year = n.year,
  month = n.month,
  crash_count = n.crash_count,
  socrata_id = n.socrata_id
  FROM n
  WHERE o.socrata_id = n.socrata_id
  RETURNING o.socrata_id
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
  SELECT socrata_id FROM upsert
  )
  '''.format(','.join(column_name_list), ','.join(vals), CARTO_CRASHES_TABLE)
  # logger.info('SQL UPSERT query:\n %s' % upsert)

  # sql = '''
  # INSERT INTO {0} ({1})
  # VALUES {2}
  # '''.format(CARTO_CRASHES_TABLE, ','.join(column_name_list), ','.join(vals))
  # logger.info('SQL INSERT statement: \n %s' % sql)

  insert_new_collision_data(upsert)


def insert_new_collision_data(query):
  """
  Takes an SQL INSERT statment and uses it with a POST request to the CARTO SQL API
  @param {query} string
  """
  payload = {'q': query, 'api_key': CARTO_API_KEY}

  try:
    r = requests.post(CARTO_SQL_API_BASEURL, data=payload)
    logger.info(r.text)
  except requests.exceptions.RequestException as e:
    logger.error(e.message)
    sys.exit(1)


def main():
  get_soda_data(get_max_date_from_carto())

if __name__ == '__main__':
  main()
