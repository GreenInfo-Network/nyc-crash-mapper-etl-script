#!/bin/env python
# -*- coding: utf-8 -*-

import requests
import os
import sys
import json


CARTO_USER_NAME = 'chekpeds'
CARTO_API_KEY = os.environ['CARTO_API_KEY'] # make sure this is available in your shell as $CARTO_API_KEY
CARTO_SQL_API_BASEURL = 'https://%s.carto.com/api/v2/sql' % CARTO_USER_NAME


def main():
    sql = """
    WITH alltypes AS (
        SELECT DISTINCT UNNEST(vehicle_type) AS vehtype
        FROM crashes_all_prod
        WHERE vehicle_type::text != '{}'
    )
    SELECT vehtype AS unknowntype FROM alltypes
    WHERE vehtype NOT IN (SELECT nyc_vehicletype FROM vehicletype_crosswalk_prod)
    ORDER BY unknowntype
    """
    unknowntypes = cartoapi_query(sql)
    unknowntypes = [ i['unknowntype'] for i in unknowntypes ]

    if not unknowntypes:
        print("All vehicle_type values are accounted for in vehicletype_crosswalk_prod")
        return

    print("Found {} vehicle_type values without corresponding crosswalk entries".format(len(unknowntypes)))
    #for unknowntype in unknowntypes:
    #    print("    {}".format(unknowntype.encode('ascii', 'replace')))
    print("")

    for unknowntype in unknowntypes:
        print(unknowntype.encode('ascii', 'replace'))

        unkarray = '\'{"' + unknowntype + '"}\'::text[]'
        sql = u"SELECT cartodb_id FROM crashes_all_prod WHERE vehicle_type && {}".format(unkarray)
        rows = cartoapi_query(sql)
        print("    Found in {} records".format(len(rows)))
        for row in rows:
            print("    cartodb_id = {}".format(row['cartodb_id']))
    print("")


def cartoapi_query(sql):
    try:
        payload = {'q': sql}
        data = requests.get(CARTO_SQL_API_BASEURL, params=payload).json()
    except requests.exceptions.RequestException as e:
        print(e.message)
        sys.exit(1)

    if 'rows' not in data:
        print('cartoapi_read() error:\n{}\n{}'.format(sql, json.dumps(data)))
        sys.exit(1)

    return data['rows']


if __name__ == '__main__':
    if not CARTO_API_KEY:
        print("No CARTO_API_KEY defined in environment")
        sys.exit(1)
    main()
    print("")
    print("ALL DONE")

