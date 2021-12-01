# Find and update latlong geometries which have since changed


### Step 1

Run the two collector scripts, which will generate two CSV files: **CrashData-CARTO.csv** and **CrashData-SODA.csv**. The SODA one takes several hours, because it must page through a few hundred `socrata_id` / `collision_id` at a time, due to URL length requirements.

```
python3 1a-fetch_carto.py
python3 1b-fetch_soda.py
```


### Step 2

Run the script to find differences between the two CSVs. This will generate a third CSV **CrashData-DIFFS.csv** which is records which have moved by over 15 meters (50 feet).

```
python3 2-make_diffs_csv.py
```


### Step 3

Look over the diffs CSV and make sure it seems sane and credible. When you proceed to the next step, you're about to edit thousands of records' locations!


### Step 4

Run the diffs in **CrashData-DIFFS.csv** at CARTO.

```
python3 4-update_carto.py
```
