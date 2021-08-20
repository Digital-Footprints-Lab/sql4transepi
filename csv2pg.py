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
        "-d", "--db", action="store", required=True,
        help="The name of the DB to work with.")
    parser.add_argument(
        "-t", "--table", action="store", required=True,
        help="The name of the table to work with.")
    parser.add_argument(
        "-f", "--file", type=argparse.FileType("r"), default=sys.stdin,
        metavar="PATH", required=True,
        help="CSV file to import.")

    args = parser.parse_args()

    return parser, args


def create_table(table, connection, cursor):

    sql = f"""CREATE TABLE IF NOT EXISTS {table} (
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


def import_csv_to_pg_table(
    csv,
    table,
    connection,
    cursor):

    dirname = os.path.dirname(__file__)
    csv_path = os.path.join(dirname, csv.name)

    sql = f"""COPY {table} (
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
        FROM '{csv_path}' CSV HEADER;"""

    try:
        cursor.execute(sql)
        connection.commit()
    except Exception as e:
        print(e)


def main():

    parser, args = args_setup()

    #~ the database can be made on the command line with
    #~ createdb [dbname]
    connection = psycopg2.connect(
    database=args.db,
    user="at9362",
    password="password",
    host="127.0.0.1",
    port="5432")

    #~ Create a cursor object using the cursor() method
    cursor = connection.cursor()


    create_table(
        args.table,
        connection,
        cursor)

    import_csv_to_pg_table(
        args.file,
        args.table,
        connection,
        cursor)

    #~ Close the connection
    connection.close()


if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        print("OK, stopping.")
