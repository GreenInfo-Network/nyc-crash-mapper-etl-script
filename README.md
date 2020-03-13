# ETL SCRIPT FOR NYC CRASH MAPPER

Extract, Transform, and Load script for fetching new data from the NYC Open Data Portal's
vehicle collision data and loading into the NYC Crash Mapper table on CARTO.


### Pre-Setup on Windows

If you run Windows, you will need to install Python; also, I recommend running under "git bash" as it provides some Unix-like tools and behaviors.

* Install Python for Windows. If you run ArcMap then you probably already have one under *C:\Python27*
* Install Git for Windows. Open a "Git bash here" for a better command line experience.
* Set some aliases in your `~/.bashrc` file, then `. ~/.bashrc`
```
PATH="$PATH:/c/Python27/ArcGIS10.5/:/c/Python27/ArcGIS10.5/Scripts/"
alias python='winpty /c/Python27/ArcGIS10.5/python.exe'
alias pip='/c/Python27/ArcGIS10.5/Scripts/pip.exe'
```


## Setup

Set the following environment variabes in your shell. Copy in the values from the Heroku panel, or from `heroku config -a nyc-crash-mapper-etl` if you use the Heroku CLI.

```
export CARTO_API_KEY='<redacted>'
export CARTO_MASTER_KEY='<redacted>'
export SOCRATA_APP_TOKEN_SECRET='<redacted>'
export SOCRATA_APP_TOKEN_PUBLIC='<redacted>'
export SENDGRID_API_KEY='<redacted>'
export SENDGRID_USERNAME='<redacted>'
export SENDGRID_TO_EMAIL="<redacted>"
```

You may find it useful to create a file called `.env` which contais these commands, then to use `source .env` to load those variables into your shell.

Install Python requirements:

```
pip install -r requirements.txt
```


## Running Locally

Run the script using Python 2.7 by doing:

```
python main.py
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
python main.py
```


## Deploying the Scheduled Task

After making changes to the script, you will want to push these changes to Heroku scheduler so the script is used the next day.

To deploy the site to the Heroku scheduler, push the code to the new remote that you added with `heroku git:remote` above:

```
git push heroku master
```
