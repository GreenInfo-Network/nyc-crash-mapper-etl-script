# ETL SCRIPT FOR NYC CRASH MAPPER

Extract, Transform, and Load script for fetching new data from the NYC Open Data Portal's
vehicle collision data and loading into the NYC Crash Mapper table on CARTO.

## Setup

Requires a `.env` file with the following:

```
CARTO_API_KEY='<redacted>'
```

Install Python requirements:

```
pip install -r requirements.txt
```

## Running Locally

To execute script locally using Python 2.7 and Foreman by doing:

```
foreman run python main.py
```

## Running via a Heroku Scheduler

To run on Heroku, make sure to:

```
heroku git:remote -a my-heroku-project-name
heroku config:set CARTO_API_KEY=<redacted>
```

Then provision the Heroku Scheduler, and add a job simply with the following command:

```
python main.py
```
