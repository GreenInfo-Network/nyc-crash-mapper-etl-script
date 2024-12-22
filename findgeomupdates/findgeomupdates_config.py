"""
common code for the smaller programs in this set
imports, config constants, the stdout logger, some shared functions
"""

# variables, including from environment
import os

CARTO_USER_NAME = 'chekpeds'
CARTO_API_KEY = os.environ['CARTO_API_KEY'] # make sure this is available in bash as $CARTO_API_KEY
CARTO_CRASHES_TABLE = 'crashes_all_prod'
CARTO_SQL_API_BASEURL = 'https://%s.carto.com/api/v2/sql' % CARTO_USER_NAME
SODA_API_COLLISIONS_BASEURL = 'https://data.cityofnewyork.us/resource/qiz3-axqb.json'

CSV_DATAFILE_SODA = 'CrashData-SODA.csv'
CSV_DATAFILE_CARTO = 'CrashData-CARTO.csv'
CSV_DATAFILE_DIFFS = 'CrashData-DIFFS.csv'

DISTANCE_THRESHOLD = 15  # if a crash has moved this far (meters) then update it


# other imports
import requests
import sys
from time import sleep
import csv


# hide the annoying InsecureRequestWarning
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)


# a function to split an array into chunks
# https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
def list_chunks(lst, n):
    return [lst[i:i + n] for i in range(0, len(lst), n)]


# wrapper to do a real-time / blocking query to CARTO's API
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
        print('performcartoquery(): Failed query\n    {}\n    {}'.format(query,  data['error']))
        sys.exit(1)


# a Haversine implementationm in Python, modified to return integer meters
# https://stackoverflow.com/questions/4913349/haversine-formula-in-python-bearing-and-distance-between-two-gps-points
def haversine(lat1, lon1, lat2, lon2):
    from math import radians, cos, sin, asin, sqrt

    R = 6372800
    dLat = radians(lat2 - lat1)
    dLon = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)

    a = sin(dLat / 2)**2 + cos(lat1) * cos(lat2) * sin(dLon / 2)**2
    c = 2 * asin(sqrt(a))

    return int(round(R * c))
