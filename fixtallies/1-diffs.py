#!/bin/env python
"""
Step 1 of the data-correction process described in issue 12
Examine the SODA and CARTO exports, matching records by socrata_id and unique_key, to find records where the injury counts differ.
"""

# the two input CSVs, and the output CSV for diffs
CSVIN_CARTO = "AllCrashes-CARTO.csv"
CSVIN_SODA  = "AllCrashes-SODA.csv"
DIFFS_OUTFILE = "crash_diffs.csv"

################################################################################################

import csv

# load both CSvs into a pair of structures
# both are keyed by the crash ID (in CARTO socrata_id, in SODA aka unique_key)
# mapping onto the crash fields, so they can easily be compared e.g. CARTO_RECORDS['1234']['injuries'] == SODA_RECORDS['1234']['injuries']
CARTO_RECORDS = {}
SODA_RECORDS = {}

print("Load {}".format(CSVIN_CARTO))
input_file = csv.DictReader(open(CSVIN_CARTO, 'rb'))
for crashinfo in input_file:
    crash_id = crashinfo['socrata_id']
    CARTO_RECORDS[crash_id] = crashinfo

print("Load {}".format(CSVIN_SODA))
input_file = csv.DictReader(open(CSVIN_SODA, 'rb'))
for crashinfo in input_file:
    crash_id = crashinfo['unique_key']
    SODA_RECORDS[crash_id] = crashinfo


print("Open target CSV {}".format(DIFFS_OUTFILE))
ofh = open(DIFFS_OUTFILE, 'wb')
diffcsvwriter = csv.writer(ofh)
diffcsvwriter.writerow([
    'socrata_id',
    'number_of_persons_injured',
    'number_of_cyclist_injured',
    'number_of_motorist_injured',
    'number_of_pedestrians_injured',
    'number_of_persons_killed',
    'number_of_cyclist_killed',
    'number_of_motorist_killed',
    'number_of_pedestrians_killed',
])


print("Comparing...")
for crash_id in CARTO_RECORDS.keys():
    ccrash = CARTO_RECORDS[crash_id]
    scrash = SODA_RECORDS.get(crash_id, None)
    if not scrash:  # known effect that some crashes are logged so long after the ETL that we never find them
        continue

    sti = scrash['number_of_persons_injured']
    cti = ccrash['number_of_persons_injured']

    sci = scrash['number_of_cyclist_injured']
    cci = ccrash['number_of_cyclist_injured']

    smi = scrash['number_of_motorist_injured']
    cmi = ccrash['number_of_motorist_injured']

    spi = scrash['number_of_pedestrians_injured']
    cpi = ccrash['number_of_pedestrian_injured']

    stk = scrash['number_of_persons_killed']
    ctk = ccrash['number_of_persons_killed']

    sck = scrash['number_of_cyclist_killed']
    cck = ccrash['number_of_cyclist_killed']

    smk = scrash['number_of_motorist_killed']
    cmk = ccrash['number_of_motorist_killed']

    spk = scrash['number_of_pedestrians_killed']
    cpk = ccrash['number_of_pedestrian_killed']

    if sti == cti and sci == cci and spi == cpi and smi == cmi and stk == ctk and sck == cck and spk == cpk and smk == cmk:  # all fields match, then we're already fine
        continue

    # generate a CSV row of this diff
    diffcsvwriter.writerow([
        scrash['unique_key'],
        scrash['number_of_persons_injured'],
        scrash['number_of_cyclist_injured'],
        scrash['number_of_motorist_injured'],
        scrash['number_of_pedestrians_injured'],
        scrash['number_of_persons_killed'],
        scrash['number_of_cyclist_killed'],
        scrash['number_of_motorist_killed'],
        scrash['number_of_pedestrians_killed'],
    ])


print("Done")
