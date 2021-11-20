# Backlog Loader

https://github.com/GreenInfo-Network/nyc-crash-mapper-etl-script/issues/7

The request is to go back through the past 36 months (January 2015 to January 2018) and look for missing crashes. Preliminary estimates are that 7,000 to 10,000 crashes may have been missed due to being "backlogged" into the Socarata Data API (SODA) weeks later than they occurred.


### Installation

This requires some non-bundled python3 libraries:

```
pip install python-dateutil
pip install requests
```


### Usage

Usage: `python3 check_backlog.py YYYY-MM`

Example: `python3 check_backlog.py 2016-07`

```
python3 check_backlog.py 2021-12
python3 check_backlog.py 2021-11
python3 check_backlog.py 2021-10
python3 check_backlog.py 2021-09
python3 check_backlog.py 2021-08
python3 check_backlog.py 2021-07
python3 check_backlog.py 2021-06
python3 check_backlog.py 2021-05
python3 check_backlog.py 2021-04
python3 check_backlog.py 2021-03
python3 check_backlog.py 2021-02
python3 check_backlog.py 2021-01

python3 check_backlog.py 2020-12
python3 check_backlog.py 2020-11
python3 check_backlog.py 2020-10
python3 check_backlog.py 2020-09
python3 check_backlog.py 2020-08
python3 check_backlog.py 2020-07
python3 check_backlog.py 2020-06
python3 check_backlog.py 2020-05
python3 check_backlog.py 2020-04
python3 check_backlog.py 2020-03
python3 check_backlog.py 2020-02
python3 check_backlog.py 2020-01

python3 check_backlog.py 2019-12
python3 check_backlog.py 2019-11
python3 check_backlog.py 2019-10
python3 check_backlog.py 2019-09
python3 check_backlog.py 2019-08
python3 check_backlog.py 2019-07
python3 check_backlog.py 2019-06
python3 check_backlog.py 2019-05
python3 check_backlog.py 2019-04
python3 check_backlog.py 2019-03
python3 check_backlog.py 2019-02
python3 check_backlog.py 2019-01

python3 check_backlog.py 2018-12
python3 check_backlog.py 2018-11
python3 check_backlog.py 2018-10
python3 check_backlog.py 2018-09
python3 check_backlog.py 2018-08
python3 check_backlog.py 2018-07
python3 check_backlog.py 2018-06
python3 check_backlog.py 2018-05
python3 check_backlog.py 2018-04
python3 check_backlog.py 2018-03
python3 check_backlog.py 2018-02
python3 check_backlog.py 2018-01

python3 check_backlog.py 2017-12
python3 check_backlog.py 2017-11
python3 check_backlog.py 2017-10
python3 check_backlog.py 2017-09
python3 check_backlog.py 2017-08
python3 check_backlog.py 2017-07
python3 check_backlog.py 2017-06
python3 check_backlog.py 2017-05
python3 check_backlog.py 2017-04
python3 check_backlog.py 2017-03
python3 check_backlog.py 2017-02
python3 check_backlog.py 2017-01

python3 check_backlog.py 2016-12
python3 check_backlog.py 2016-11
python3 check_backlog.py 2016-10
python3 check_backlog.py 2016-09
python3 check_backlog.py 2016-08
python3 check_backlog.py 2016-07
python3 check_backlog.py 2016-06
python3 check_backlog.py 2016-05
python3 check_backlog.py 2016-04
python3 check_backlog.py 2016-03
python3 check_backlog.py 2016-02
python3 check_backlog.py 2016-01
```
