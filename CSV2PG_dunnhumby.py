
#~ Standard library imports
import sys
import os
import re
import argparse
from string import Template
import csv

#~ 3rd party imports
import pandas as pd
import psycopg2
from psycopg2 import Error

#~ local imports
import db_config
import PG_status


def args_setup():

    parser = argparse.ArgumentParser(
        description="Postgres DB Importer: Dunn Humby Tescos Transaction Datasets.",
        epilog="Example: python csv2sql.py -i items.csv")
    parser.add_argument(
        "-i", "--input", type=argparse.FileType("r"), default=sys.stdin,
        metavar="PATH", required=True,
        help="CSV file to import.")

    args = parser.parse_args()

    return parser, args


def create_table(connection, cursor):

    """Create a table consistent with the column names
    for the CSVs in the Dunn Hunby Tesco example datasets"""

    sql = Template("""
        CREATE TABLE IF NOT EXISTS $table (
        SHOP_WEEK INT,
        SHOP_DATE INT,
        SHOP_WEEKDAY INT,
        SHOP_HOUR INT,
        QUANTITY INT,
        SPEND MONEY,
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
        STORE_REGION VARCHAR);""")

    try:
        cursor.execute(sql.substitute(table="dunn_humby"))
        connection.commit()
    except Exception as e:
        print(e)


def import_csv_to_pg_table(
    csv,
    connection,
    cursor):

    """Imports a CSV with columns named from the Dunn Hunby
    Tesco example datasets"""

    print(f"Importing {csv.name} to Postgres table 'dunn_humby', just a moment...")

    dirname = os.path.dirname(__file__)
    csv_path = os.path.join(dirname, csv.name)

    sql = Template("""
        COPY $table (
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
        FROM '$csv_path' CSV HEADER;""")

    try:
        cursor.execute(sql.substitute(table="dunn_humby", csv_path=csv_path))
        connection.commit()
        print(f"\nOK, {csv.name} imported.")
    except Exception as e:
        print(f"\n!!! Import failed: {csv.name} is not consistent with table fields.")
        print(f"!!! The csv format might not be correct, or you might be importing to the wrong table?")
        sys.exit(1)

def db_details(
    connection,
    cursor):

    """Return some information about the DB after scrape import to Postgres.
    """

    sql_record_count = Template("""
        SELECT COUNT(*)
        FROM $table;""")
    sql_column_count = Template("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name='$table';""")
    sql_cust_count = Template("""
        SELECT COUNT (DISTINCT CUST_CODE) FROM $table;""")
    sql_basket_count = Template("""
        SELECT COUNT (DISTINCT BASKET_ID) FROM $table;""")
    sql_date_count = Template("""
        SELECT COUNT (DISTINCT SHOP_DATE) FROM $table;""")

    cursor.execute(sql_record_count.substitute(table="dunn_humby"))
    record_count = cursor.fetchall()
    cursor.execute(sql_column_count.substitute(table="dunn_humby"))
    column_count = cursor.fetchall()
    cursor.execute(sql_cust_count.substitute(table="dunn_humby"))
    cust_count = cursor.fetchall()
    cursor.execute(sql_basket_count.substitute(table="dunn_humby"))
    basket_count = cursor.fetchall()
    cursor.execute(sql_date_count.substitute(table="dunn_humby"))
    date_count = cursor.fetchall()
    print(f"\ndunn_humby details:\nRecords:     {record_count[0][0]}")
    print(f"Columns:     {column_count[0][0]}")
    print(f"Customers:   {cust_count[0][0]}")
    print(f"Baskets:     {basket_count[0][0]}")
    print(f"Shop dates:  {date_count[0][0]}")


def main():

    if len(sys.argv) < 2:
        print("\nPostgres DB Importer: Dunn Humby Transaction Deatils.")
        print("Please provide an input file, for example:")
        print("\npython dunnhumby_CSV2PG.py -i items.csv")
        sys.exit(1)

    parser, args = args_setup()

    #~ Create connection using psycopg2
    connection, cursor = PG_status.connect_to_postgres(db_config)

    create_table(
        connection,
        cursor)

    import_csv_to_pg_table(
        args.input,
        connection,
        cursor)

    try:
        db_details(
            connection,
            cursor)
    except:
        print("\n!!!There doesn't seem to be a table present.")

    connection.close()


if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        print("OK, stopping.")
