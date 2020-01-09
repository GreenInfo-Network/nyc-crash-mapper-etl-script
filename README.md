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

Requires a `.env` file with the following. Look up the values from the Heroku panel and copy them in.

```
export CARTO_API_KEY='<redacted>'
export CARTO_MASTER_KEY='<redacted>'
export SOCRATA_APP_TOKEN_SECRET='<redacted>'
export SOCRATA_APP_TOKEN_PUBLIC='<redacted>'
```

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

To run on Heroku, fill in the values and send them to Heroku via:

```
heroku git:remote -a nyc-crash-mapper-etl
heroku config:set CARTO_API_KEY=<redacted>
heroku config:set CARTO_MASTER_KEY=<redacted>
heroku config:set SOCRATA_APP_TOKEN_SECRET=<redacted>
heroku config:set SOCRATA_APP_TOKEN_PUBLIC=<redacted>
```

Then provision the Heroku Scheduler, and add a job simply with the following command:

```
python main.py
```
