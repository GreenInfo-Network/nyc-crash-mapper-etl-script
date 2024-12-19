#!/bin/env python3
"""
Step 2. Compare the CARTO CSV and the SODA CSV, find coordinates which have changed substantially.
"""

from findgeomupdates_config import *


def run():
    print("Loading Carto CSV")
    existing_records = {}  # socrata_id => crashdata
    with open(CSV_DATAFILE_CARTO) as fh:
        spamreader = csv.DictReader(fh)
        for row in spamreader:
            id = row['socrata_id']
            existing_records[id] = row
    print("    Loaded {} pre-existing rows".format(len(existing_records)))

    print("Loading Socrata CSV")
    potential_updates = []
    with open(CSV_DATAFILE_SODA) as fh:
        spamreader = csv.DictReader(fh)
        potential_updates = [row for row in spamreader if row['collision_id'] in existing_records]
    print("    Loaded {} potential updates".format(len(potential_updates)))

    print("Finding changes over {} meters".format(DISTANCE_THRESHOLD))
    updates = []
    for row in potential_updates:
        id = row['collision_id']
        old = existing_records[id]

        lat_old = float(old['lat'])
        lng_old = float(old['lng'])
        lat_new = float(row['latitude'])
        lng_new = float(row['longitude'])
        meters = haversine(lat_old, lng_old, lat_new, lng_new)
        underthreshold = meters <= DISTANCE_THRESHOLD

        if False:  # change to True, False, or underthreshold, to tune the verbose debugging
            print("    {}    {}    {} meters    ({}, {}, {}, {})".format(
                row['collision_id'],
                row['crash_date'],
                meters,
                lat_old, lng_old, lat_new, lng_new
            ))

        if underthreshold:
            continue

        updates.append({
            'socrata_id': old['socrata_id'],
            'cartodb_id': old['cartodb_id'],
            'date_val': old['date_val'],
            'lat_new': lat_new,
            'lng_new': lng_new,
            'lat_old': lat_old,
            'lng_old': lng_old,
            'metersdiff': meters,
        })

    print("Found {} records to update".format(len(updates)))

    print("Writing CSV {}".format(CSV_DATAFILE_DIFFS))
    with open(CSV_DATAFILE_DIFFS, 'w') as fh:
        spamwriter = csv.writer(fh)

        spamwriter.writerow([
            'socrata_id',
            'cartodb_id',
            'date_val',
            'lat_new',
            'lng_new',
            'lat_old',
            'lng_old',
            'metersdiff',
        ])

        for row in updates:
            spamwriter.writerow([
                row['socrata_id'],
                row['cartodb_id'],
                row['date_val'],
                row['lat_new'],
                row['lng_new'],
                row['lat_old'],
                row['lng_old'],
                row['metersdiff'],
            ])

    # done
    print("")
    print("Done")
    print("Review the diffs CSV {}".format(CSV_DATAFILE_DIFFS))
    print("Then proceed to step 3.")


if __name__ == '__main__':
    if not CARTO_API_KEY:
        print("No CARTO_API_KEY defined in environment")
        sys.exit(1)
    run()
