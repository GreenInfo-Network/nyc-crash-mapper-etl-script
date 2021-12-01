#!/bin/env python3
"""
Step 1. Go through CARTO and SODA and fetch records, then write them out to CSVs.
"""

from findgeomupdates_config import *


def run():
    print("Loading Carto CSV")
    with open(CSV_DATAFILE_CARTO) as fh:
        spamreader = csv.DictReader(fh)
        socrata_ids = [row['socrata_id'] for row in spamreader]
    print("Loaded {} rows to check".format(len(socrata_ids)))

    soda_rows = []
    soda_chunk_size = 500
    soda_chunks = list_chunks(socrata_ids, soda_chunk_size)
    done = 0
    print("Querying SODA, {} records per page".format(soda_chunk_size))
    for thesecrashids in soda_chunks:
        done += 1
        print("    {} of {}".format(done, len(soda_chunks)))
        soda_rows += getsodaforcrashids(thesecrashids)
        sleep(5)

    print("Writing CSV {}".format(CSV_DATAFILE_SODA))
    with open(CSV_DATAFILE_SODA, 'w') as fh:
        spamwriter = csv.writer(fh)

        spamwriter.writerow([
            'collision_id',
            'crash_date',
            'longitude',
            'latitude',
        ])

        for row in soda_rows:
            # GDA
            # strange but true, we saw at least one
            if 'collision_id' not in row:
                print("SODA row with no collision_id")
                print(row)
                continue
            # GDA

            spamwriter.writerow([
                row['collision_id'],
                row['crash_date'],
                row['longitude'],
                row['latitude'],
            ])

    # done
    print("")
    print("Done with this step. Proceed to step 2.")


def getsodaforcrashids(collisionids):
    # fetch SODA for the specified collision_id list
    # the latlong-not-null is added because it was noted that SODA doesn't in fact have latlong for some records
    # and such records would not be useful, being less data than we already have! (geocoding session back in 2017?)
    try:
        whereclause = "collision_id IN ({}) AND latitude IS NOT NULL AND latitude != '0'".format(','.join([str(i) for i in collisionids]))
        crashdata = requests.get(
            SODA_API_COLLISIONS_BASEURL,
            params={
                '$where': whereclause,
                '$order': 'collision_id ASC',
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


if __name__ == '__main__':
    if not CARTO_API_KEY:
        print("No CARTO_API_KEY defined in environment")
        sys.exit(1)
    run()
