# Backlog Loader

https://github.com/GreenInfo-Network/nyc-crash-mapper-etl-script/issues/7

The request is to go back through the past 36 months (January 2015 to January 2018) and look for missing crashes. Preliminary estimates are that 7,000 to 10,000 crashes may have been missed due to being "backlogged" into the Socarata Data API (SODA) weeks later than they occurred.


### Installation

This requires some non-bundled Python libraries:

```
pip install python-dateutil
pip install requests
```


### Usage

Usage: `python check_backlog.py YYYY-MM`

Example: `python check_backlog.py 2016-07`

Complete list:
```
python check_backlog.py 2017-12
python check_backlog.py 2017-11
python check_backlog.py 2017-10
python check_backlog.py 2017-09
python check_backlog.py 2017-08
python check_backlog.py 2017-07
python check_backlog.py 2017-06
python check_backlog.py 2017-05
python check_backlog.py 2017-04
python check_backlog.py 2017-03
python check_backlog.py 2017-02
python check_backlog.py 2017-01
python check_backlog.py 2016-12
python check_backlog.py 2016-11
python check_backlog.py 2016-10
python check_backlog.py 2016-09
python check_backlog.py 2016-08
python check_backlog.py 2016-07
python check_backlog.py 2016-06
python check_backlog.py 2016-05
python check_backlog.py 2016-04
python check_backlog.py 2016-03
python check_backlog.py 2016-02
python check_backlog.py 2016-01
python check_backlog.py 2015-12
python check_backlog.py 2015-11
python check_backlog.py 2015-10
python check_backlog.py 2015-09
python check_backlog.py 2015-08
python check_backlog.py 2015-07
python check_backlog.py 2015-06
python check_backlog.py 2015-05
python check_backlog.py 2015-04
python check_backlog.py 2015-03
python check_backlog.py 2015-02
python check_backlog.py 2015-01
```
