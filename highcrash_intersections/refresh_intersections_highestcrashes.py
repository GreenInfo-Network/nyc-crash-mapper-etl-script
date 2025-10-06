#!/bin/env python3
"""
Repopulate the nyc_highcrash_intersections table as the 100 intersections with the highest crashcount.
"""

import requests
from time import sleep
import os
import logging
import sys
import time
import json


CARTO_USER_NAME = 'chekpeds'
CARTO_API_KEY = os.environ['CARTO_API_KEY'] # make sure this is available in bash as $CARTO_API_KEY
CARTO_MASTER_KEY = os.environ['CARTO_MASTER_KEY'] # make sure this is available in bash as $CARTO_MASTER_KEY
CARTO_SQL_API_BASEURL = 'https://%s.carto.com/api/v2/sql' % CARTO_USER_NAME
CARTO_BATCH_API_BASEURL = 'https://%s.carto.com/api/v2/sql/job' % CARTO_USER_NAME

DBTABLE_ALL_INTERSECTIONS = 'nyc_intersections'
DBTABLE_THEWORSTONES = 'nyc_highcrash_intersections'

HOWMANY_INTERSECTIONS = 500

# CREATE_OR_REFRESH = 'create'
CREATE_OR_REFRESH = 'refresh'

################################################################################################

# documentation https://cartodb.github.io/developers/sql-api/guides/batch-queries/#fetching-job-results
# Batch API does not work with SELECT statements, except for "SELECT INTO" aka "CREATE TABLE AS"

def run():
    if CREATE_OR_REFRESH == 'create':
        # run this batch to create the materialized view; this should only need to happen the first time
        # this took about 10 minutes, AFTER the job changed into running state, as of October 2025
        logger.info("Starting batch job to crdeate")
        jobid = start_carto_batchjob([
            "DROP MATERIALIZED VIEW {}".format(DBTABLE_THEWORSTONES),
            "CREATE MATERIALIZED VIEW {} AS SELECT * FROM {} WHERE crashcount IS NOT NULL ORDER BY crashcount DESC LIMIT {}".format(DBTABLE_THEWORSTONES, DBTABLE_ALL_INTERSECTIONS, HOWMANY_INTERSECTIONS),
            "GRANT SELECT ON {} TO PUBLIC".format(DBTABLE_THEWORSTONES),
            "CREATE INDEX {}_the_geom ON {} (the_geom)".format(DBTABLE_THEWORSTONES, DBTABLE_THEWORSTONES),
            "CREATE INDEX {}_the_geom_webmercator ON {} (the_geom_webmercator)".format(DBTABLE_THEWORSTONES, DBTABLE_THEWORSTONES),\
        ])
        results = wait_carto_batchjob(jobid)
        logger.info( results )

    if CREATE_OR_REFRESH == 'refresh':
        # run this batch to create the materialized view; this should only need to happen the first time
        # this took about 10 minutes, AFTER the job changed into running state, as of October 2025
        logger.info("Starting batch job to refresh")
        jobid = start_carto_batchjob([
            "REFRESH MATERIALIZED VIEW {}".format(DBTABLE_THEWORSTONES),
        ])
        results = wait_carto_batchjob(jobid)
        logger.info( results )

    logger.info("Fetching results back")
    try:
        thetopintersections = requests.get(
            CARTO_SQL_API_BASEURL,
            params={
                'q': "SELECT * FROM {}".format(DBTABLE_THEWORSTONES),
            }
        ).json()
    except requests.exceptions.RequestException as e:
        logger.error(e.message)
        sys.exit(1)
    if not 'rows' in thetopintersections or not len(thetopintersections['rows']):
        logger.error('No resulting rows: {0}'.format(json.dumps(thetopintersections)))
        sys.exit(1)

    for thisone in thetopintersections['rows']:
        logger.info([thisone['name'], thisone['borough'], thisone['crashcount']])

    logger.info("DONE")


def start_carto_batchjob(querylist):
    # logger.info(query)

    url = "{}?api_key={}".format(CARTO_BATCH_API_BASEURL, CARTO_MASTER_KEY)
    jsonbody = {
        'query': querylist,
    }

    try:
        r = requests.post(url, json=jsonbody)
        jobinfo = r.json()
        if 'error' in jobinfo and jobinfo['error']:
            raise ValueError(jobinfo['error'])
        jobid = jobinfo['job_id']
        logger.info('CARTO Batch Job ID: {}'.format(jobid))
        return jobid
    except requests.exceptions.RequestException as e:
        logger.error(e.message)
        sys.exit(1)
    except Exception as e:
        logger.error(e.message)
        sys.exit(1)


def status_carto_batchjob(jobid):
    # simply fetch and return the status of a CartoDB batch job
    url = "{}/{}?api_key={}".format(CARTO_BATCH_API_BASEURL, jobid, CARTO_MASTER_KEY)
    jobstatus = requests.get(url).json()
    return jobstatus['status']


def wait_carto_batchjob(jobid, waitseconds=30):
    # loop and wait, blocking until the batch job has completed
    url = "{}/{}?api_key={}".format(CARTO_BATCH_API_BASEURL, jobid, CARTO_MASTER_KEY)
    logger.info("Waiting for batch job {} to complete".format(jobid))

    while True:
        time.sleep(waitseconds)

        jobstatus = requests.get(url).json()
        logger.info("Status of batch job {} is {}".format(jobid, jobstatus['status']))

        if jobstatus['status'] == 'running' or jobstatus['status'] == 'pending':  # still running, give it another sleep-loop
            continue
        elif jobstatus['status'] == 'done':  # yay! break which will implicitly return
            break
        elif jobstatus['status'] == 'failed':  # failed, throw a fit and exit
            errmessage = "Batch job {} failed: {}".format(jobid, jobstatus['failed_reason'])
            logger.error(errmessage)
            sys.exit(1)
        else:  # unexpected condition, throw a fit
            errmessage = "Batch job {} exited with unknown status: {}".format(jobid, jobstatus['status'])
            logger.error(errmessage)
            sys.exit(1)

    return jobstatus  # return the whole query/job metadata


logging.basicConfig(
    level=logging.INFO,
    format=' %(asctime)s - %(levelname)s - %(message)s',
    datefmt='%I:%M:%S %p')
logger = logging.getLogger()


if __name__ == '__main__':
    if not CARTO_MASTER_KEY:
        logger.info("No CARTO_MASTER_KEY defined in environment")
        sys.exit(1)
    if not CARTO_API_KEY:
        logger.info("No CARTO_API_KEY defined in environment")
        sys.exit(1)
    run()
