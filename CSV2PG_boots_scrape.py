
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

#~ local imports
import db_config
import PG_ops


def args_setup():

    parser = argparse.ArgumentParser(
        description="Postgres DB Importer: Boots Products, website scrape.",
        epilog="Example: python csv2sql.py -t table1 -i monday_scrape.csv")
    parser.add_argument(
        "-i", "--input", type=argparse.FileType("r"), default=sys.stdin,
        metavar="PATH", required=True,
        help="CSV file to import.")

    args = parser.parse_args()

    return parser, args


def create_scrape_table(connection, cursor):

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
        cursor.execute(sql.substitute(table="boots_products"))
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
    csv,
    connection,
    cursor):

    """Imports a CSV with columns named from the Boots scraper"""

    print(f"Importing {csv.name} to Postgres table 'boots_products', just a moment...")

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
        cursor.execute(sql.substitute(table="boots_products"))
        connection.commit()
        print(f"\nOK, {csv.name} imported.")
        #~ remove the temp table
        cursor.execute("""DROP TABLE IF EXISTS temp_table;""")
        connection.commit()
    except Exception as e:
        print(f"\n!!! Import failed: {csv.name} is not consistent with table fields.")
        print(f"!!! The csv format might not be correct, or you might be importing to the wrong table?")
        sys.exit(1)


def db_scrape_details(
    connection,
    cursor):

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
        cursor.execute(sql_column_count.substitute(table="boots_products"))
        column_count = cursor.fetchall()
        cursor.execute(sql_record_count.substitute(table="boots_products"))
        record_count = cursor.fetchall()
        cursor.execute(sql_product_count.substitute(table="boots_products"))
        product_count = cursor.fetchall()
        print(f"\nboots_products details:\nColumns:     {column_count[0][0]}")
        print(f"Records:     {record_count[0][0]}")
        print(f"Products:    {product_count[0][0]}")
        if record_count[0][0] != product_count[0][0]:
            discrepancy = record_count[0][0] - product_count[0][0]
            print(f"\n!!! Note: record and product counts differ by {discrepancy}. \nThis may be due to products having null id codes?")
    except Exception as e:
        print(e)


def main():

    if len(sys.argv) < 2:
        print("\nPostgres DB Importer: Boots Product Details.")
        print("Please provide an input file, for example:")
        print("\npython boots_scrape_CSV2PG.py -i scrape211101.csv")
        sys.exit(1)

    parser, args = args_setup()

    #~ Create connection using psycopg2
    connection, cursor = PG_ops.connect_to_postgres(db_config)

    create_scrape_table(
        connection,
        cursor)

    import_scrape_csv_to_pg_table(
        args.input,
        connection,
        cursor)

    db_scrape_details(
        connection,
        cursor)

    connection.close()


if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        print("OK, stopping.")
