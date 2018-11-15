This is a helper program to "bootstrap" the initial set of linkages between crashes and a newly-created set of districts.

That is to say:
* There exists a table such as `nyc_assembly` and its `identifier` field is how we ID them
* The `crashes_all_prod` DB table has a field such as `assembly` and this contains a polygon `identifier` so we can relate crashes to those polygonal areas
* We need to update the `crashes_all_prod` `assembly` field to contain these identifiers, ...
* ... but the `UPDATE SET FROM WHERE` technique used in the ETL script takes too long with a wholly-new set of 1.5M rows, and CARTO terminates it after **5 seconds**.

So we need to do this iteratively, in tiny chunks so as to fall under CARTO's platform limits. This is tedious and annoying, but when we're trying to bootstrap a new field with 1.5M rows in under 5 seconds, it's how we have to do things.


## Using it

* Ensure that the new polygons table exists, e.g. `nyc_assembly` and that it has an `identififer` field (integer)

* Ensure that the `crashes_all_prod` table has the linkage field, e.g. `assembly` (integer)

* Open up `linkthem.py` and set the `POLYGONS_TABLE` and `CRASH_FIELD` constants, matching to this new table and the field

* Ensure that your environment has the `CARTO_API_KEY` variable set, e.g. `export CARTO_API_KEY="abc123secret321xyz"`

* Run `python linkthem.py` This will show various statistics, and will ask for confirmation before proceeding.


## Tips

This will only update crash records where the field is currently `NULL`. Because of this behavior, if this program is terminated by an error or by hitting Ctrl-C, records which have already been updated will not need a second update. The query will still run, but will update 0 rows and will be fairly speedy.

While it's running, you can log in to CARTO and monitor progress with a query like this:
```
SELECT assembly, count(*) AS howmany
FROM crashes_all_prod
GROUP BY assembly
ORDER BY assembly
```

Not all of the queries will have the same run times, and they can vary widely. More complex geometries may take longer, so some polygons that worked with `HOWMANY_CHUNKS = 10` may timeout and require `HOWMANY_CHUNKS = 20` It is not recommended to set `HOWMANY_CHUNKS` lowe than 20 for this reason: the 1.5-second queries you're seeing, may be followed by 4.0-second queries for a larger or more complex polygon later in the same run.
