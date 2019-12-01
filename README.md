# ETL SCRIPT FOR NYC CRASH MAPPER

Extract, Transform, and Load script for fetching new data from the NYC Open Data Portal's
vehicle collision data and loading into the NYC Crash Mapper table on CARTO.

## Setup

Requires a `.env` file with the following:

```
CARTO_API_KEY='<redacted>'
SOCRATA_APP_TOKEN_SECRET='<redacted>'
SOCRATA_APP_TOKEN_PUBLIC='<redacted>'
```

Install Python requirements:

```
pip install -r requirements.txt
```

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
* Install foreman for running tasks: `npm install -g foreman` Then remember that command-lines starting with "foreman" should start with "nf" instead.


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
heroku config:set SOCRATA_APP_TOKEN_SECRET=<redacted>
```

Then provision the Heroku Scheduler, and add a job simply with the following command:

```
python main.py
```
