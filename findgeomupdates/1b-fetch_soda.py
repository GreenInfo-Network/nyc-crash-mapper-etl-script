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
    print(f"Loaded {len(socrata_ids)} rows to check")

    soda_rows = []
    soda_chunk_size = 500
    soda_chunks = list_chunks(socrata_ids, soda_chunk_size)
    done = 0
    print(f"Querying SODA, {soda_chunk_size} records per page")
    for thesecrashids in soda_chunks:
        done += 1
        while True:
            try:
                print(f"    {done} of {len(soda_chunks)}")

                thesecrashidstring = ','.join([str(i) for i in thesecrashids])
                thesecrashdata = requests.get(
                    SODA_API_COLLISIONS_BASEURL,
                    params={
                        '$where': f"collision_id IN ({thesecrashidstring})",
                        '$order': 'collision_id ASC',
                        '$limit': '50000',  # their default is something low like 100 so specify their highest cap here; in fact we send chunks of 500 (soda_chunk_size)
                    },
                    verify=False  # requests hates the SSL certificate due to hostname mismatch, but it IS valid
                ).json()

                soda_rows += thesecrashdata
                sleep(5)
                break
            except:  # GDA
                print("        Oops, retrying")
                sleep(20)

    print(f"Writing CSV {CSV_DATAFILE_SODA}")
    with open(CSV_DATAFILE_SODA, 'w') as fh:
        spamwriter = csv.writer(fh)

        spamwriter.writerow([
            'collision_id',
            'crash_date',
            'longitude',
            'latitude',
        ])

        for row in soda_rows:
            # strange but true, we saw at least one
            if 'collision_id' not in row:
                print("SODA row with no collision_id")
                print(row)
                continue

            spamwriter.writerow([
                row['collision_id'],
                row['crash_date'],
                row['longitude'] if 'longitude' in row else '',
                row['latitude'] if 'latitude' in row else '',
            ])

    # done
    print("")
    print("Done with this step. Proceed to step 2.")


if __name__ == '__main__':
    if not CARTO_API_KEY:
        print("No CARTO_API_KEY defined in environment")
        sys.exit(1)
    run()
