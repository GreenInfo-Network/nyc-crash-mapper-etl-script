# ETL SCRIPT FOR NYC CRASH MAPPER

Extract, Transform, and Load script for fetching data from the NYC Open Data Portal's
vehicle collision data and loading into the NYC Crash Mapper table on CARTO.

Requires a `.env` file with the following:

```
CARTO_API_KEY='<redacted>'
```

Execute script using Python 2.7 and Foreman by doing:

```
foreman run python main.py
```
