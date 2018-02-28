#!/bin/env python
"""
Step 2 of the data-correction process described in issue 12
Examine the SODA and CARTO exports, matching records by socrata_id and unique_key, to find records where the injury counts differ.
"""

import csv
import sys
import os
import requests
from time import sleep

# the input CSV with corrected injury/fatality counts for records that need it
DIFFS_CSVILE = "crash_diffs.csv"

# CARTO connection info: URLs and API keys
CARTO_USER_NAME = 'chekpeds'
CARTO_API_KEY = os.environ['CARTO_API_KEY'] # make sure this is available in bash as $CARTO_API_KEY
CARTO_CRASHES_TABLE = 'crashes_all_prod'
CARTO_SQL_API_BASEURL = 'https://%s.carto.com/api/v2/sql' % CARTO_USER_NAME

################################################################################################

def run():
    print("Load {}".format(DIFFS_CSVILE))
    input_file = csv.DictReader(open(DIFFS_CSVILE, 'rb'))

    howmanydone = 0

    for crashinfo in input_file:
        sql = """
            UPDATE {table} SET
            number_of_persons_injured={number_of_persons_injured},
            number_of_cyclist_injured={number_of_cyclist_injured},
            number_of_motorist_injured={number_of_motorist_injured},
            number_of_pedestrian_injured={number_of_pedestrians_injured},
            number_of_persons_killed={number_of_persons_killed},
            number_of_cyclist_killed={number_of_cyclist_killed},
            number_of_motorist_killed={number_of_motorist_killed},
            number_of_pedestrian_killed={number_of_pedestrians_killed}
            WHERE socrata_id={socrata_id}
        """.format(
            table=CARTO_CRASHES_TABLE,
            number_of_persons_injured=crashinfo['number_of_persons_injured'],
            number_of_cyclist_injured=crashinfo['number_of_cyclist_injured'],
            number_of_motorist_injured=crashinfo['number_of_motorist_injured'],
            number_of_pedestrians_injured=crashinfo['number_of_pedestrians_injured'],
            number_of_persons_killed=crashinfo['number_of_persons_killed'],
            number_of_cyclist_killed=crashinfo['number_of_cyclist_killed'],
            number_of_motorist_killed=crashinfo['number_of_motorist_killed'],
            number_of_pedestrians_killed=crashinfo['number_of_pedestrians_killed'],
            socrata_id=crashinfo['socrata_id'],
        ).strip()

        print(crashinfo['socrata_id'])

        # print(sql)
        performcartoquery(sql)
        sleep(1)

        # periodic maintenance: progress readout, and a VACUUM
        howmanydone += 1
        if howmanydone % 100 == 0:
            print("PROGRESS: {}".format(howmanydone))
            sleep(5)
        if howmanydone % 2500 == 0:
            performcartoquery("VACUUM {}".format(CARTO_CRASHES_TABLE))
            sleep(20)

    # done
    performcartoquery("VACUUM FULL {}".format(CARTO_CRASHES_TABLE))


def performcartoquery(query):
    # POST the given SQL query to CARTO
    payload = {'q': query, 'api_key': CARTO_API_KEY}

    try:
        r = requests.post(CARTO_SQL_API_BASEURL, data=payload)
        print(r.text)
    except requests.exceptions.RequestException as e:
        print(e.message)
        sys.exit(2)


if __name__ == '__main__':
    if not CARTO_API_KEY:
        print("No CARTO_API_KEY defined in environment")
        sys.exit(1)
    run()

