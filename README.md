# ETL SCRIPT FOR NYC CRASH MAPPER

Extract, Transform, and Load script for fetching new data from the NYC Open Data Portal's vehicle collision data and loading into the NYC Crash Mapper table on CARTO.


## Python

This script is written for Python 3.8 The `python` and `pip` commands below reflect this.


## Setup

Set the following environment variables in your shell. Copy in the values from the Heroku panel, or from `heroku config -a nyc-crash-mapper-etl` if you use the Heroku CLI.

```
export CARTO_API_KEY='<redacted>'
export CARTO_MASTER_KEY='<redacted>'
export SOCRATA_APP_TOKEN_SECRET='<redacted>'
export SOCRATA_APP_TOKEN_PUBLIC='<redacted>'
export SENDGRID_API_KEY='<redacted>'
export SENDGRID_USERNAME='<redacted>'
export SENDGRID_TO_EMAIL="<redacted>"
```

You may find it useful to create a file called `.env` which contains these commands, then to use `source .env` to load those variables into your shell.

Double check that the variable was set and is in your environment:
```
echo $SENDGRID_USERNAME
```

Install Python requirements:

```
pip3.8 install -r requirements.txt
```


## Running Locally

Run the script using Python 2.7 by doing:

```
python3.8 main.py
```


## Running via a Heroku Scheduler

To run on Heroku, fill in the values and send them to Heroku via commands such as these. Include all of the variables in that environment variable list described above.

```
heroku git:remote -a nyc-crash-mapper-etl

heroku config:set CARTO_API_KEY=<redacted>
heroku config:set CARTO_MASTER_KEY=<redacted>
heroku config:set SOCRATA_APP_TOKEN_SECRET=<redacted>
heroku config:set SOCRATA_APP_TOKEN_PUBLIC=<redacted>
heroku config:set SENDGRID_API_KEY='<redacted>'
heroku config:set SENDGRID_USERNAME='<redacted>'
heroku config:set SENDGRID_TO_EMAIL="<redacted>"
```

Then provision the Heroku Scheduler, and add a job simply with the following command:

```
python3.8 main.py
```


## Deploying the Scheduled Task

After making changes to the script, you will want to push these changes to Heroku scheduler so the script is used the next day.

To deploy the site to the Heroku scheduler, push the code to the Heroku remote:

```
heroku git:remote -a nyc-crash-mapper-etl
```

```
git push heroku master
```

## Note about qgtunnel

In 2023, we needed to have a static IP for this service, so that it could be safelisted for use with a MySQL database the client is using for the Walkmapper project. Heroku does not offer static IPs itself, but there's an addon for it. the `.qgtunnel` file in the root of this repo is the config for that. Settings and docs are reachable via the add-on section of the control panel on heroku.com.
