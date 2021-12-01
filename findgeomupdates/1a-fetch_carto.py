#!/bin/env python3
"""
Step 1. Go through CARTO and SODA and fetch records, then write them out to CSVs.
"""

from findgeomupdates_config import *


def run():
    print("Querying Carto")
    sql = """
    SELECT socrata_id, cartodb_id, date_val, ST_X(the_geom) AS lng, ST_Y(the_geom) AS lat
    FROM {}
    WHERE socrata_id IS NOT NULL AND date_val >= '{}T00:00:00Z' AND the_geom IS NOT NULL
    ORDER BY socrata_id
    """.format(
        CARTO_CRASHES_TABLE,
        MIN_DATE
    )
    cartodb_rows = performcartoquery(sql)
    print("{} rows have geometry".format(len(cartodb_rows)))

    print("Writing CSV {}".format(CSV_DATAFILE_CARTO))
    with open(CSV_DATAFILE_CARTO, 'w') as fh:
        spamwriter = csv.writer(fh)

        spamwriter.writerow([
            'socrata_id',
            'cartodb_id',
            'date_val',
            'lng',
            'lat',
        ])

        for row in cartodb_rows:
            spamwriter.writerow([
                row['socrata_id'],
                row['cartodb_id'],
                row['date_val'],
                row['lng'],
                row['lat'],
            ])

    # done
    print("")
    print("Done with this step. Proceed to step 1B.")


if __name__ == '__main__':
    if not CARTO_API_KEY:
        print("No CARTO_API_KEY defined in environment")
        sys.exit(1)
    run()
