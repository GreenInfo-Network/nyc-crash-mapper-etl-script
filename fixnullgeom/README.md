About 12% of records from Socrata have no `latitude` and `longitude` fields, and therefore are imported into CARTO with `the_geom` being NULL.

This script will look over those records in CARTO, then check Socrata to see whether a latitude & longitude are now given for them.

Some general notes about Socarata/SODA data updates, during the big run in November 2021:
* Prior to 2016, very few updates have been done (as of Q4 2021) and it seems unlikely that more are forthcoming. There are about 101K records with null geom prior to 2016-01-01.
* After 2017, it's very inconsistent how many updates to expect. Throughout 2016-2020, many months have fewer than 20 updates, while several had 50-100 or 100+.
* Data for 2016 and 2017 had a major revisit & update at Socrata some time in 2018 - 2020. This November Q4 2021 run found a lot of updates for this period, on the order of 1000+ per month for 2016 and January 20-17, with a few hundred per month for most of 2017.
* Generally, data updates for past months appear to be ongoing. During this initial run. I noted that repeating a month's run the next day would sometimes bring in a few (under 10) new results since the day before. However, no such updates were noted prior to 2016.


Usage: `python3 fix_null_geom_in_carto.py YYYY-MM`

```
python3 fix_null_geom_in_carto.py 2016-01
python3 fix_null_geom_in_carto.py 2016-02
python3 fix_null_geom_in_carto.py 2016-03
python3 fix_null_geom_in_carto.py 2016-04
python3 fix_null_geom_in_carto.py 2016-05
python3 fix_null_geom_in_carto.py 2016-06
python3 fix_null_geom_in_carto.py 2016-07
python3 fix_null_geom_in_carto.py 2016-08
python3 fix_null_geom_in_carto.py 2016-09
python3 fix_null_geom_in_carto.py 2016-10
python3 fix_null_geom_in_carto.py 2016-11
python3 fix_null_geom_in_carto.py 2016-12

python3 fix_null_geom_in_carto.py 2017-01
python3 fix_null_geom_in_carto.py 2017-02
python3 fix_null_geom_in_carto.py 2017-03
python3 fix_null_geom_in_carto.py 2017-04
python3 fix_null_geom_in_carto.py 2017-05
python3 fix_null_geom_in_carto.py 2017-06
python3 fix_null_geom_in_carto.py 2017-07
python3 fix_null_geom_in_carto.py 2017-08
python3 fix_null_geom_in_carto.py 2017-09
python3 fix_null_geom_in_carto.py 2017-10
python3 fix_null_geom_in_carto.py 2017-11
python3 fix_null_geom_in_carto.py 2017-12

python3 fix_null_geom_in_carto.py 2018-01
python3 fix_null_geom_in_carto.py 2018-02
python3 fix_null_geom_in_carto.py 2018-03
python3 fix_null_geom_in_carto.py 2018-04
python3 fix_null_geom_in_carto.py 2018-05
python3 fix_null_geom_in_carto.py 2018-06
python3 fix_null_geom_in_carto.py 2018-07
python3 fix_null_geom_in_carto.py 2018-08
python3 fix_null_geom_in_carto.py 2018-09
python3 fix_null_geom_in_carto.py 2018-10
python3 fix_null_geom_in_carto.py 2018-11
python3 fix_null_geom_in_carto.py 2018-12

python3 fix_null_geom_in_carto.py 2019-01
python3 fix_null_geom_in_carto.py 2019-02
python3 fix_null_geom_in_carto.py 2019-03
python3 fix_null_geom_in_carto.py 2019-04
python3 fix_null_geom_in_carto.py 2019-05
python3 fix_null_geom_in_carto.py 2019-06
python3 fix_null_geom_in_carto.py 2019-07
python3 fix_null_geom_in_carto.py 2019-08
python3 fix_null_geom_in_carto.py 2019-09
python3 fix_null_geom_in_carto.py 2019-10
python3 fix_null_geom_in_carto.py 2019-11
python3 fix_null_geom_in_carto.py 2019-12

python3 fix_null_geom_in_carto.py 2020-01
python3 fix_null_geom_in_carto.py 2020-02
python3 fix_null_geom_in_carto.py 2020-03
python3 fix_null_geom_in_carto.py 2020-04
python3 fix_null_geom_in_carto.py 2020-05
python3 fix_null_geom_in_carto.py 2020-06
python3 fix_null_geom_in_carto.py 2020-07
python3 fix_null_geom_in_carto.py 2020-08
python3 fix_null_geom_in_carto.py 2020-09
python3 fix_null_geom_in_carto.py 2020-10
python3 fix_null_geom_in_carto.py 2020-11
python3 fix_null_geom_in_carto.py 2020-12

python3 fix_null_geom_in_carto.py 2021-01
python3 fix_null_geom_in_carto.py 2021-02
python3 fix_null_geom_in_carto.py 2021-03
python3 fix_null_geom_in_carto.py 2021-04
python3 fix_null_geom_in_carto.py 2021-05
python3 fix_null_geom_in_carto.py 2021-06
python3 fix_null_geom_in_carto.py 2021-07
python3 fix_null_geom_in_carto.py 2021-08
python3 fix_null_geom_in_carto.py 2021-09
```
