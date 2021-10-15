
#~ Standard library imports
import sys
import os
import re
import argparse
from string import Template
import csv

#~ 3rd party imports
import psycopg2
from psycopg2 import Error


def args_setup():

    parser = argparse.ArgumentParser(
        description="Postgres DB Importer: Boots Products, website scrape.",
        epilog="Example: python csv2sql.py -d database1 -t table1 -i monday_scrape.csv")
    parser.add_argument(
        "-d", "--db", action="store", required=True,
        help="The name of the DB to work with.")
    parser.add_argument(
        "-t", "--table", action="store", required=True,
        help="The name of the table to work with.")
    parser.add_argument(
        "-i", "--input", type=argparse.FileType("r"), default=sys.stdin,
        metavar="PATH", required=True,
        help="CSV file to import.")

    args = parser.parse_args()

    return parser, args


def create_table(table, connection, cursor):

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
        cursor.execute(sql.substitute(table=table))
        connection.commit()
    except Exception as e:
        print(e)


def create_scrape_table(table, connection, cursor):

    """Create a table consistent with the column names
    for the Boots scraper"""

    #! PRICE as VARCHAR is a workaround given "£" in price.
    sql = Template("""
        CREATE TABLE IF NOT EXISTS $table (
        IDX1 INT,
        PRODUCT_LINK VARCHAR,
        PRODUCTID INT UNIQUE,
        NAME VARCHAR,
        PRICE VARCHAR,
        DETAILS VARCHAR,
        LONG_DESCRIPTION VARCHAR);""")

    try:
        cursor.execute(sql.substitute(table=table))
        connection.commit()
    except Exception as e:
        print(e)

    #~ build a temp table to deal with duplicates
    sql = """
        CREATE TABLE IF NOT EXISTS temp_table (
        IDX1 INT,
        PRODUCT_LINK VARCHAR,
        PRODUCTID INT,
        NAME VARCHAR,
        PRICE VARCHAR,
        DETAILS VARCHAR,
        LONG_DESCRIPTION VARCHAR);"""
    try:
        cursor.execute(sql)
        connection.commit()
    except Exception as e:
        print(e)

def import_scrape_csv_to_pg_table(
    db,
    csv,
    table,
    connection,
    cursor):

    """Imports a CSV with columns named from the Boots scraper"""

    print(f"Importing {csv.name} to Postgres DB '{db}', table '{table}', just a moment...")

    dirname = os.path.dirname(__file__)
    csv_path = os.path.join(dirname, csv.name)

    #~ COPY into temp table first, which can deal with dups
    sql = Template("""
        COPY temp_table (
        IDX1,
        PRODUCT_LINK,
        PRODUCTID,
        NAME,
        PRICE,
        DETAILS,
        LONG_DESCRIPTION)
        FROM '$csv_path' CSV HEADER;""")

    try:
        cursor.execute(sql.substitute(csv_path=csv_path))
        connection.commit()
        print(f"\n{csv.name} imported to staging area, removing duplicates...")
    except Exception as e:
        print(e)

    #~ then INSERT to true table, rejecting dups on productid
    sql = Template("""
        INSERT INTO $table
        SELECT * FROM temp_table
        ON CONFLICT DO NOTHING;""")

    try:
        cursor.execute(sql.substitute(table=table))
        connection.commit()
        print(f"\nOK, {csv.name} imported.")
        #~ remove the temp table
        cursor.execute("""DROP TABLE IF EXISTS temp_table;""")
        connection.commit()
    except Exception as e:
        print(e)


def db_scrape_details(
    db,
    table,
    cursor,
    connection):

    """
    Return some information about the current state of Postgres.
    """

    sql_column_count = Template("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name='$table';""")
    sql_record_count = Template("""
        SELECT COUNT(*)
        FROM $table;""")
    sql_product_count = Template("""
        SELECT COUNT (DISTINCT PRODUCTID) FROM $table;""")

    try:
        cursor.execute(sql_column_count.substitute(table=table))
        column_count = cursor.fetchall()
        cursor.execute(sql_record_count.substitute(table=table))
        record_count = cursor.fetchall()
        cursor.execute(sql_product_count.substitute(table=table))
        product_count = cursor.fetchall()
        print(f"\n{table} details:\nColumns:     {column_count[0][0]}")
        print(f"Records:     {record_count[0][0]}")
        print(f"Products:    {product_count[0][0]}")
        if record_count[0][0] != product_count[0][0]:
            discrepancy = record_count[0][0] - product_count[0][0]
            print(f"\n!!! Note: record and product counts differ by {discrepancy}. \nThis may be due to products having null id codes?")
    except Exception as e:
        print(e)


def main():

    parser, args = args_setup()

    #~ Create connection using psycopg2
    try:
        connection = psycopg2.connect(
            database=args.db,
            user="at9362",
            password="password",
            host="127.0.0.1",
            port="5432")
    except psycopg2.OperationalError as e:
        print(f"\n!!! {e}")
        print(f"You can create the DB on the command line with:")
        print(f"createdb {args.db}")
        sys.exit(1)

    #~ Check for disallowed characters in table name
    if args.table[0].isnumeric() or not re.match("^[a-zA-Z0-9_]+$", args.table):
        print("\n!!! Table names cannot start with a number, or include symbols except_underscores.")
        sys.exit(1)

    #~ Create a cursor object
    cursor = connection.cursor()

    create_scrape_table(
        args.table,
        connection,
        cursor)

    import_scrape_csv_to_pg_table(
        args.db,
        args.input,
        args.table,
        connection,
        cursor)

    db_scrape_details(
        args.db,
        args.table,
        cursor,
        connection)

    connection.close()


if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        print("OK, stopping.")
