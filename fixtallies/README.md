## Load SODA Injury Counts for Past Crashes, and Update CARTO

Reference: https://github.com/GreenInfo-Network/nyc-crash-mapper-etl-script/issues/12#issuecomment-368610780

The ETL script **up through February 2017** was logging records with incorrect vehicle-specific injury tallies (`number_of_cyclist_injured, number_of_pedestrian_injured, number_of_motorist_injured`) particularly the `number_of_motorist_injured` field often being logged at 2X to 4X the stated number given by SODA. As a result, the front-end apps are giving inaccurate injury counts.

This script will correct that, by comparing a data dump from CARTO with a data dump from SODA (records are identified by CARTO `socrata_id` compared to SODA `unique_key`), to find records where the jonury counts in CARTO do not match those in SODA. It will then generate the SQL queries necessary to update the CARTO records, bringing them into line with NYC's data at SODA.

### Date Range and Summary Stats

The target date range for this run is **date >= '2015-01-01 date < '2017-03-01'** At the time this is being written, data starting in March 2017 are looking just fine.

* 478636 crashes in the SODA CSV
* 478618 crashes in the CARTO CSV
* 87408 crashes (18%) had a mismatched value in any of the 8 injury fields


### Data Prep

*Get a CSV from CARTO*

Load the following query, and use the Export tool to fetch the results as CSV. Save it as **AllCrashes-CARTO.csv**

```
SELECT
    socrata_id,
    number_of_persons_injured,
    number_of_cyclist_injured,
    number_of_motorist_injured,
    number_of_pedestrian_injured,
    number_of_persons_killed,
    number_of_cyclist_killed,
    number_of_motorist_killed,
    number_of_pedestrian_killed
FROM crashes_all_prod
WHERE
    socrata_id IS NOT NULL
    AND date_val::date >= '2015-01-01' AND date_val::date < '2017-03-01'
    ORDER BY date_val
```

*Get a CSV from Socrata Data API (SODA)*

Download a CSV from SODA using an URL like this. Save it as **AllCrashes-SODA.csv**

```
https://data.cityofnewyork.us/resource/qiz3-axqb.csv?$where=date%3E=%272015-01-01%27%20AND%20date%3C%272017-03-01%27&$order=date%20ASC&$limit=1000000
```


### Running It

Run `python 1-diffs.py` This will examine the SODA-provided injury & fatality counts, and the CARTO injury & fatality counts, to find records which do not match. Thus, the generated **crash_diffs.csv** file will lack records which are already correct. This will be considerably smaller than either source CSV, as a condition of 0 injuries + 0 fatalities is pretty common, and tends to have been entered correctly.

Check out the **crash_diffs.csv** file, which is the new injury counts for the crashes that need updating. Pick out a few which you have previously identified as anomalous, and see if the new numbers look better.

Run `2-update_carto.py` to load the **crash_diffs.csv** content into CARTO, updating the given records.

This process can take quite some time: throttled to about 1 query per second, and with the API timing out or otherwise failing now and then.


### Doing It Again

During the run of CARTO updates, some of them likely failed and went unnoticed. The CARTO API has a rate limit and can time out sometimes, so not every query ran correctly.

So, run this whole thing again and this time you should get only a few diffs.

And, because that run may have missed a few, do it again.

After 3 runs or so, you'll have so few diffs that you can indeed watch them all and confirm that they all worked.
