import sys
import os
import re
import argparse
import csv
import pandas as pd
import psycopg2
from psycopg2 import Error


def args_setup():

    parser = argparse.ArgumentParser(
        description="Postgres DB Importer and Updater",
        epilog="Example: python csv2sql.py -i items.csv -d database1.db -t table")
    parser.add_argument(
        "-f", "--file", type=argparse.FileType("r"), default=sys.stdin,
        metavar="PATH", required=True,
        help="CSV file to import.")
    parser.add_argument(
        "-d", "--db", action="store", required=True,
        help="The name of the DB to work with.")
    parser.add_argument(
        "-t", "--table", action="store", required=True,
        help="The name of the table to work with.")
    parser.add_argument( #! todo
        "-i", "--index", action="store",
        help="The name of the table to work with.")

    args = parser.parse_args()

    return parser, args


def create_table(connection, cursor):

    sql = f"""CREATE TABLE table3(
        SHOP_WEEK INT,
        SHOP_DATE INT,
        SHOP_WEEKDAY INT,
        SHOP_HOUR INT,
        QUANTITY INT,
        SPEND REAL,
        PROD_CODE VARCHAR,
        PROD_CODE_10 VARCHAR,
        PROD_CODE_20 VARCHAR,
        PROD_CODE_30 VARCHAR,
        PROD_CODE_40 VARCHAR,
        CUST_CODE VARCHAR,
        CUST_PRICE_SENSITIVITY VARCHAR,
        CUST_LIFESTAGE VARCHAR,
        BASKET_ID VARCHAR,
        BASKET_SIZE VARCHAR,
        BASKET_PRICE_SENSITIVITY VARCHAR,
        BASKET_TYPE TEXT,
        BASKET_DOMINANT_MISSION TEXT,
        STORE_CODE VARCHAR,
        STORE_FORMAT VARCHAR,
        STORE_REGION VARCHAR);"""

    try:
        cursor.execute(sql)
        connection.commit()
    except Exception as e:
        print(e)

"""
this will import fine, but only if the table is new. we get a
psycopg2.errors.InFailedSqlTransaction error if we try to import to
an existing, populated table"""
#! weird. if we JUST try to create an existing table, we get a "already there"
#! if we JUST try importing to existing table, everything goes fine (makes dups at the moment)
#! but if we do BOTH create and import, we get the InFailedSqlTransaction error.
#! also, if the import comes first but then try to make table, there is no problem (import fine, then "already there").

def import_csv_to_pg_table(
    csv,
    table,
    connection,
    cursor):

    sql = f"""COPY table3 (
        SHOP_WEEK,
        SHOP_DATE,
        SHOP_WEEKDAY,
        SHOP_HOUR,
        QUANTITY,
        SPEND,
        PROD_CODE,
        PROD_CODE_10,
        PROD_CODE_20,
        PROD_CODE_30,
        PROD_CODE_40,
        CUST_CODE,
        CUST_PRICE_SENSITIVITY,
        CUST_LIFESTAGE,
        BASKET_ID,
        BASKET_SIZE,
        BASKET_PRICE_SENSITIVITY,
        BASKET_TYPE,
        BASKET_DOMINANT_MISSION,
        STORE_CODE,
        STORE_FORMAT,
        STORE_REGION)
        FROM '/Users/at9362/Code/sql4transepi/first100records.csv' CSV HEADER;"""

    cursor.execute(sql)
    connection.commit()


def main():

    #~ the database can be made on the command line with
    #~ createdb [dbname]
    connection = psycopg2.connect(
    database="test20aug",
    user="at9362",
    password="password",
    host="127.0.0.1",
    port="5432")

    #~ Create a cursor object using the cursor() method
    cursor = connection.cursor()

    create_table(connection, cursor)

    csv="transactions_200626.csv",
    table="table1",
    import_csv_to_pg_table(
        csv,
        table,
        connection,
        cursor)

    #~ Closing the connection
    connection.close()


if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        print("OK, stopping.")
