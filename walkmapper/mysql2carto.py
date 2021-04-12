#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# set DB credentials in envfile OR set them in environment otherwise
# import environment variables:   source walkmapper/envfile
# call this script via CLI from parent directory:   python3 walkmapper/mysql2carto.py
# this script relies on several environment variables for CARTO and for the MySQL DB where obstructions are stored

from os import environ as ENV

CARTO_USER_NAME = 'chekpeds'
CARTO_SQL_API_BASEURL = 'https://%s.carto.com/api/v2/sql' % CARTO_USER_NAME
CARTO_API_KEY = ENV['WALKMAPPER_ETL_CARTO_API_KEY']

DB_HOST = ENV['WALKOBSTRUCTION_MYSQL_HOST']
DB_PORT = ENV['WALKOBSTRUCTION_MYSQL_PORT']
DB_USER = ENV['WALKOBSTRUCTION_MYSQL_USER']
DB_PASS = ENV['WALKOBSTRUCTION_MYSQL_PASS']
DB_NAME = ENV['WALKOBSTRUCTION_MYSQL_NAME']

# to quickly find whether a record has changed, a concat() of these fields is done and compared
SUMMARY_FIELDS_CARTO = [
    'isFirstTime', "COALESCE(createdAt::varchar, '0000-00-00')",
    'isSecondTime', "COALESCE(secondTimeSendDate::varchar, '0000-00-00')",
    'isThirdTime', "COALESCE(thirdTimeSendDate::varchar, '0000-00-00')",
    'isCompleted', "COALESCE(completedDate::varchar, '0000-00-00')",
    'image1', 'image2', 'image3', 'image4', 'image5',  # these come last, see MySQL one for why
]
SUMMARY_FIELDS_MYSQL = [
    "CASE WHEN isFirstTime > 0 THEN 't' ELSE 'f' END", 'DATE(createdAt)',
    "CASE WHEN isSecondTime > 0 THEN 't' ELSE 'f' END", 'DATE(secondTimeSendDate)',
    "CASE WHEN isThirdTime > 0 THEN 't' ELSE 'f' END", 'DATE(thirdTimeSendDate)',
    "CASE WHEN isCompleted > 0 THEN 't' ELSE 'f' END", 'DATE(completedDate)',
    # image1 image2 image3 image4 image5 - these come last beause they're added in code, sorry!
]

# when records are inserted to CARTO we also fill in fields with the names of things they intersect: asembly district number, etc.
BOUNDARY_INTERSECTIONS = [
    { 'targetnamefield': "borough", 'polygontable': "nyc_borough", 'polygonname': "borough" },
    { 'targetnamefield': "community_board", 'polygontable': "nyc_community_board", 'polygonname': "identifier" },
    { 'targetnamefield': "city_council", 'polygontable': "nyc_city_council", 'polygonname': "identifier" },
    { 'targetnamefield': "neighborhood", 'polygontable': "nyc_neighborhood", 'polygonname': "identifier" },
    { 'targetnamefield': "nypd_precinct", 'polygontable': "nyc_nypd_precinct", 'polygonname': "identifier" },
    { 'targetnamefield': "assembly", 'polygontable': "nyc_assembly", 'polygonname': "identifier" },
    { 'targetnamefield': "senate", 'polygontable': "nyc_senate", 'polygonname': "identifier" },
    { 'targetnamefield': "businessdistrict", 'polygontable': "nyc_businessdistrict", 'polygonname': "bidistrict" },
]


########################################################################################################################


import requests
import MySQLdb
import MySQLdb.cursors
import logging
import sys
import json


class ObstructionMyqlToCartoLoader:
    def __init__(self):
        # set up and initialization, e.g. connect to MySQL DB
        self.db = MySQLdb.connect(host=DB_HOST, port=int(DB_PORT), user=DB_USER, password=DB_PASS, database=DB_NAME, cursorclass=MySQLdb.cursors.DictCursor)

        self.cartoapiurl = CARTO_SQL_API_BASEURL
        self.cartoapikey = CARTO_API_KEY


    def run(self):
        # look over the 2 tables and create lists:   self.records_to_insert   self.records_to_update   self.records_to_skip
        self.fetch_mysql_obstruction_records()

        """GDA testing
        for row in self.records_to_insert:
            self.insert_record_to_carto(row)
            self.intersect_boundaries(row)
        for row in self.records_to_update:
            self.update_record_in_carto(row)
        testing GDA"""
        for row in self.records_to_delete:
            self.delete_record_in_carto(row)


    def run_carto_query(self, sqlquery):
        try:
            params = {
                'q': sqlquery,
                'api_key': self.cartoapikey,
            }
            reply = requests.get(self.cartoapiurl, params=params).json()

            if 'rows' not in reply:
                raise requests.exceptions.RequestException("No rows found in returned data: {}".format(json.dumps(reply)))

            return reply
        except requests.exceptions.RequestException as e:
            logger.error(e)
            sys.exit(1)


    def fetch_mysql_obstruction_records(self):
        # from CARTO get a list of all "id" AND a "summary" of some key fields, for all obstructions aready known
        # this is how we distinguish inserts from updates, and updates that won't in fact change anything
        alreadyincarto = {}
        gotten = self.run_carto_query("""
            SELECT id, CONCAT({summaryfields}) AS summary FROM walkmapper_obstructions
        """.format(
            summaryfields=','.join(SUMMARY_FIELDS_CARTO)
        ))
        for row in gotten['rows']:
            alreadyincarto[ int(row['id']) ] = row['summary']
        logger.info("Found {howmany} obstructions in CARTO".format(howmany=len(alreadyincarto)))

        # from MySQL fetch a list of all obstructions in their system, as well as a "summary"
        # do some data type corrections: datetime objects to ISO strings, float fields as floats, bytes as strings, ...
        with self.db.cursor() as cursor:
            sql = """
            SELECT
                o.id,
                o.obstructionLat, o.obstructionLong,
                CONCAT(o.buildingNumber, ' ', o.streetName) AS address,
                o.obstructionAddressLine AS locationdetail,
                c2.name AS topcategory, c1.name AS subcategory,
                o.createdAt, o.secondTimeSendDate, o.thirdTimeSendDate,
                o.isFirstTime, o.isSecondTime, o.isThirdTime,
                o.isCompleted, o.completedDate,
                CONCAT({summaryfields}) AS summary
            FROM
                obstructionDetails o,
                categoryMaster c1, categoryMaster c2
            WHERE
                o.categoryId = c1.id AND c1.parentId = c2.id
            AND NOT o.isDelete
            """.format(
                summaryfields=','.join(SUMMARY_FIELDS_MYSQL)
            )

            found_obstructions = []
            cursor.execute(sql)
            for row in cursor.fetchall():
                row['id'] = int(row['id'])

                row['obstructionLat'] = float(row['obstructionLat']) 
                row['obstructionLong'] = float(row['obstructionLong'])

                row['createdAt'] = row['createdAt'].date().isoformat() if row['createdAt'] else None
                row['completedDate'] = row['completedDate'].date().isoformat() if row['completedDate'] else None
                row['secondTimeSendDate'] = row['secondTimeSendDate'].date().isoformat() if row['secondTimeSendDate'] else None
                row['thirdTimeSendDate'] = row['thirdTimeSendDate'].date().isoformat() if row['thirdTimeSendDate'] else None

                row['isFirstTime'] = int(row['isFirstTime']) > 0
                row['isSecondTime'] = int(row['isSecondTime']) > 0
                row['isThirdTime'] = int(row['isThirdTime']) > 0
                row['isCompleted'] = int(row['isCompleted']) > 0

                row['address'] = str(row['address'])
                row['locationdetail'] = str(row['locationdetail'])
                row['topcategory'] = str(row['topcategory'])
                row['subcategory'] = str(row['subcategory'])

                found_obstructions.append(row)

            logger.info("Found {howmany} obstructions in MySQL".format(howmany=len(found_obstructions)))

        # .. but then a second pass to collect photos into the records as image1 through image5
        # they're using MySQL 5 which doens't support CTEs nor window functions, so we have to do it the long & slow way
        # populate the 5 imageX fields, then recalculate the summary field
        for row in found_obstructions:
            with self.db.cursor() as cursor:
                row['image1'] = None
                row['image2'] = None
                row['image3'] = None
                row['image4'] = None
                row['image5'] = None

                sql = """
                SELECT image FROM obstructionImagesDetails
                WHERE obstructionId={id}
                ORDER BY id
                LIMIT 5
                """.format(
                    id = row['id'],
                )
                cursor.execute(sql)

                images = cursor.fetchall()
                if len(images) >= 1:
                    row['image1'] = images[0]['image']
                if len(images) >= 2:
                    row['image2'] = images[1]['image']
                if len(images) >= 3:
                    row['image3'] = images[2]['image']
                if len(images) >= 4:
                    row['image4'] = images[3]['image']
                if len(images) >= 5:
                    row['image6'] = images[4]['image']

                row['summary'] += row['image1'] if row['image1'] else ''
                row['summary'] += row['image2'] if row['image2'] else ''
                row['summary'] += row['image3'] if row['image3'] else ''
                row['summary'] += row['image4'] if row['image4'] else ''
                row['summary'] += row['image5'] if row['image5'] else ''

        # go through all of the found_obstructions from MySQL and check its "summary" field vs the one from CARTO
        # not found = insert
        # different = update
        # same = no change, skip
        self.records_to_insert = []
        self.records_to_update = []
        self.records_to_skip = []

        for thisone in found_obstructions:
            if thisone['id'] not in alreadyincarto:
                self.records_to_insert.append(thisone)
            elif thisone['summary'] != alreadyincarto[ int(thisone['id']) ]:
                self.records_to_update.append(thisone)
            else:
                self.records_to_skip.append(thisone)

        # look for MySQL records with isDelete=1    these are to be deleted from the CARTO end
        self.records_to_delete = []
        with self.db.cursor() as cursor:
            sql = """
            SELECT
                id,
                obstructionLat, obstructionLong,
                CONCAT(buildingNumber, ' ', streetName) AS address,
                obstructionAddressLine AS locationdetail
            FROM
                obstructionDetails
            WHERE isDelete
            """
            cursor.execute(sql)

            for row in cursor.fetchall():
                if row['id'] in alreadyincarto:
                    row['id'] = int(row['id'])
                    row['obstructionLat'] = float(row['obstructionLat']) 
                    row['obstructionLong'] = float(row['obstructionLong'])
                    self.records_to_delete.append(row)


        # done, summary stats
        logger.info("Sorted: {} to skip".format(len(self.records_to_skip)))
        logger.info("Sorted: {} to insert".format(len(self.records_to_insert)))
        logger.info("Sorted: {} to update".format(len(self.records_to_update)))
        logger.info("Sorted: {} to delete".format(len(self.records_to_delete)))


    def escape_string(self, string):
        # wrapper around MySQL escape (works for PostgreSQL too, for any string we'll encounter)
        # and around Python 3's bytes behavior
        if string is None:
            return ''
        return str(MySQLdb.escape_string(string), 'utf-8')


    def quote_value(self, value):
        # add 'quotes' around the string, unless it's True, False, None in which case return a PostgreSQL-compatible non-quoted string
        if value is None:
            return 'NULL'
        if value is True:
            return 'TRUE'
        if value is False:
            return 'FALSE'
        return "'{}'".format(self.escape_string(value))


    def update_record_in_carto(self, row):
        # CARTO DB API doesn't do parameterized queries, so be sure to sanitize anything we didn't sanitize above
        print("UPDATE for obstruction ID {}".format(row['id']))

        sql = """
        UPDATE walkmapper_obstructions SET
            createdat = {createdat},
            isfirsttime = {isfirsttime},
            issecondtime = {issecondtime},
            secondtimesenddate = {secondtimesenddate},
            isthirdtime = {isthirdtime},
            thirdtimesenddate = {thirdtimesenddate},
            iscompleted = {iscompleted},
            completeddate = {completeddate},
            image1 = {image1},
            image2 = {image2},
            image3 = {image3},
            image4 = {image4},
            image5 = {image5}
        WHERE id = {id}
        """.format(
            id = row['id'],
            topcategory = self.quote_value(row['topcategory']),
            subcategory = self.quote_value(row['subcategory']),
            createdat = self.quote_value(row['createdAt']),
            secondtimesenddate = self.quote_value(row['secondTimeSendDate']),
            thirdtimesenddate = self.quote_value(row['thirdTimeSendDate']),
            completeddate = self.quote_value(row['completedDate']),
            isfirsttime = self.quote_value(row['isFirstTime']),
            issecondtime = self.quote_value(row['isSecondTime']),
            isthirdtime = self.quote_value(row['isThirdTime']),
            iscompleted = self.quote_value(row['isCompleted']),
            image1 = self.quote_value(row['image1']),
            image2 = self.quote_value(row['image2']),
            image3 = self.quote_value(row['image3']),
            image4 = self.quote_value(row['image4']),
            image5 = self.quote_value(row['image5']),
        )

        self.run_carto_query(sql)


    def insert_record_to_carto(self, row):
        # CARTO DB API doesn't do parameterized queries, so be sure to sanitize anything we didn't sanitize above
        print("INSERT for obstruction ID {}".format(row['id']))

        sql = """
        INSERT INTO walkmapper_obstructions (
            id,
            obstructionlat, obstructionlong, the_geom,
            address, locationdetail,
            topcategory, subcategory,
            createdat, secondtimesenddate, thirdtimesenddate, completeddate,
            isfirsttime, issecondtime, isthirdtime, iscompleted,
            image1, image2, image3, image4, image5
        ) VALUES (
            {id},
            {obstructionlat}, {obstructionlong}, ST_POINTFROMTEXT('POINT({obstructionlong} {obstructionlat})',4326),
            {address}, {locationdetail},
            {topcategory}, {subcategory},
            {createdat}, {secondtimesenddate}, {thirdtimesenddate}, {completeddate},
            {isfirsttime}, {issecondtime}, {isthirdtime}, {iscompleted},
            {image1}, {image2}, {image3}, {image4}, {image5}
        )
        """.format(
            id = row['id'],
            obstructionlat = row['obstructionLat'],
            obstructionlong = row['obstructionLong'],
            address = self.quote_value(row['address']),
            locationdetail = self.quote_value(row['locationdetail']),
            topcategory = self.quote_value(row['topcategory']),
            subcategory = self.quote_value(row['subcategory']),
            createdat = self.quote_value(row['createdAt']),
            secondtimesenddate = self.quote_value(row['secondTimeSendDate']),
            thirdtimesenddate = self.quote_value(row['thirdTimeSendDate']),
            completeddate = self.quote_value(row['completedDate']),
            isfirsttime = self.quote_value(row['isFirstTime']),
            issecondtime = self.quote_value(row['isSecondTime']),
            isthirdtime = self.quote_value(row['isThirdTime']),
            iscompleted = self.quote_value(row['isCompleted']),
            image1 = self.quote_value(row['image1']),
            image2 = self.quote_value(row['image2']),
            image3 = self.quote_value(row['image3']),
            image4 = self.quote_value(row['image4']),
            image5 = self.quote_value(row['image5']),
        )

        self.run_carto_query(sql)


    def delete_record_in_carto(self, row):
        # CARTO DB API doesn't do parameterized queries, so be sure to sanitize anything we didn't sanitize above
        print("DELETE for obstruction ID {}".format(row['id']))

        sql = """
        DELETE FROM walkmapper_obstructions WHERE id={id}
        """.format(
            id = row['id'],
        )

        self.run_carto_query(sql)


    def intersect_boundaries(self, row):
        print("BOUNDS for obstruction ID {}".format(row['id']))

        for boundinfo in BOUNDARY_INTERSECTIONS:
            print("    {}".format(boundinfo['polygontable']))

            sql = """
            UPDATE walkmapper_obstructions
            SET {targetnamefield} = poly.{polygonname}
            FROM {polygontable} poly
            WHERE ST_WITHIN(walkmapper_obstructions.the_geom, poly.the_geom)
            AND walkmapper_obstructions.id = {id}
            """.format(
                id = row['id'],
                polygontable = boundinfo['polygontable'],
                polygonname = boundinfo['polygonname'],
                targetnamefield = boundinfo['targetnamefield'],
            )
            self.run_carto_query(sql)



if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format=' %(asctime)s - %(levelname)s - %(message)s',
        datefmt='%I:%M:%S %p')
    logger = logging.getLogger()

    if not CARTO_API_KEY:
        logger.info("No CARTO api key defined in environment")
        sys.exit(1)

    ObstructionMyqlToCartoLoader().run()

    logger.info('ALL DONE')
