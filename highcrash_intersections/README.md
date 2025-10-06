# Refresh Table of Intersections With Highest Crash Count

Refer to https://github.com/GreenInfo-Network/nyc-crash-mapper/issues/112

The DB table `nyc_intersections` is circles at intersections throughout NYC. There are 40,202 of them.

Problem is, aggregating and reporting when there are 40k records just takes too long to do within CARTO's API limits (the 30-second time limit, which seems to happen at 25 seconds nowadays), and therefore the Map and Viz applications do not work when Intersections is selected as the area type.

Solution: Pick the 100 intersections with the most crashes and load them into a separate table `nyc_highcrash_intersections`, and have the Map and Vis use that for the reporting. This loses a lot of detail as to intersections with 1-2 crashes, which is the large majority of them, but allows the Intersections functionality to continue and be reframed as "the most dangerous intersections."

Running it:
```
python refresh_intersections_highestcrashes.py
```

This will take a while as it queries, then will empty and repopulate the `nyc_highcrash_intersections` DB table.

It is expected that we will re-run this every year or two, to pick at that time the 100 most dangerous intersections.


## Discussion

The `nyc_intersections` table already has a field `crashcount` which is the number of crashes at that intersection in the last two years, and this is updated by the nightly ETL script. From this the query is rather easy: `SELECT * FROM nyc_intersections ORDER BY crashcount DESC LIMIT 100`

We then save the resulting records to the `nyc_highcrash_intersections` table, after emptying it.

This is done via CARTO's Batch API, since the 30-second time limit is not sufficient.
