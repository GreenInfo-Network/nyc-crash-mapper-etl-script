#!/bin/env python3
"""
Step 3. Go through the diffs CSV and update those records. Also, clear their polygon assignments so they will be picked up in the next nightly run.
"""

from findgeomupdates_config import *


def run():
    print("Loading diffs CSV")
    updates = []
    with open(CSV_DATAFILE_DIFFS) as fh:
        spamreader = csv.DictReader(fh)
        updates = [row for row in spamreader]
    print("    Loaded {} updates".format(len(updates)))

    done = 0
    for row in updates:
        done += 1
        print("{} / {}    {}    {}    {} meters".format(
            done,
            len(updates),
            row['socrata_id'],
            row['date_val'],
            row['metersdiff'],
        ))

        sql = """
        UPDATE {}
        SET
            the_geom=ST_SETSRID(ST_GEOMFROMTEXT('POINT({} {})'), 4326),
            longitude={}, latitude={},
            borough=NULL,
            city_council=NULL,
            senate=NULL,
            assembly=NULL,
            businessdistrict=NULL,
            community_board=NULL,
            neighborhood=NULL,
            nypd_precinct=NULL
        WHERE socrata_id={}
        """.format(
            CARTO_CRASHES_TABLE,
            row['lng_new'], row['lat_new'],
            row['lng_new'], row['lat_new'],
            row['socrata_id']
        )

        performcartoquery(sql)
        sleep(5)

    print("DONE")


if __name__ == '__main__':
    if not CARTO_API_KEY:
        print("No CARTO_API_KEY defined in environment")
        sys.exit(1)
    run()
