
#~ Standard library imports
import sys
import os
import re
import argparse
from string import Template
import csv
import codecs
import shutil

#~ 3rd party imports
import pandas as pd
import psycopg2
from psycopg2 import Error


def args_setup():

    parser = argparse.ArgumentParser(
        description="Postgres DB Importer: Boots Advantage Loyalty Cards.",
        epilog="Example: python csv2sql.py -d database1 -t table1 -i boots_card.csv")
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

    """Create a table consistent with CSVs returned from Boot
    after an information request. 211004: this csv needs to first
    be converted from UTF16 to UTF8/ASCII, and from tab delimited
    to comma separated..."""

    sql = Template("""
        CREATE TABLE IF NOT EXISTS $table (
        ID BIGINT,
        DATE2 DATE,
        TIME3 TIME,
        STORE VARCHAR,
        PAYMENT VARCHAR,
        STAFF_DISCOUNT_CARD_NUMBER INT,
        ITEM_CODE INT,
        ITEM_DESCRIPTION VARCHAR,
        POINTS_ADJUSTMENT INT,
        POINTS_ITEM REAL,
        UNITS INT,
        SPEND MONEY,
        DISCOUNT REAL);""")

    try:
        cursor.execute(sql.substitute(table=table))
        connection.commit()
    except Exception as e:
        print(e)


def import_csv_to_pg_table(
    db,
    csv,
    table,
    connection,
    cursor):

    """Imports a CSV with columns consistent with Boots loyalty card CSV columns"""

    print(f"Importing Boots Card {csv.name} to Postgres DB '{db}', table '{table}', just a moment...")

    dirname = os.path.dirname(__file__)
    csv_path = os.path.join(dirname, "UTF8" + csv.name)

    sql = Template("""
        COPY $table (
        ID,
        DATE2,
        TIME3,
        STORE,
        PAYMENT,
        STAFF_DISCOUNT_CARD_NUMBER,
        ITEM_CODE,
        ITEM_DESCRIPTION,
        POINTS_ADJUSTMENT,
        POINTS_ITEM,
        UNITS,
        SPEND,
        DISCOUNT)
        FROM '$csv_path' CSV HEADER;""")

    try:
        cursor.execute(sql.substitute(table=table, csv_path=csv_path))
        connection.commit()
        print(f"\nOK, {csv.name} imported.")
    except Exception as e:
        print(e)


def db_details(
    db,
    table,
    cursor,
    connection):

    """
    Return some information about the Boots card import to Postgres.
    """

    sql_record_count = Template("""
        SELECT COUNT(*)
        FROM $table;""")
    sql_column_count = Template("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name='$table';""")
    sql_id_count = Template("""
        SELECT COUNT (DISTINCT ID) FROM $table;""")
    sql_item_count = Template("""
        SELECT COUNT (DISTINCT ITEM_CODE) FROM $table;""")
    sql_date_count = Template("""
        SELECT COUNT (DISTINCT DATE2) FROM $table;""")

    try:
        cursor.execute(sql_record_count.substitute(table=table))
        record_count = cursor.fetchall()
        cursor.execute(sql_column_count.substitute(table=table))
        column_count = cursor.fetchall()
        cursor.execute(sql_id_count.substitute(table=table))
        id_count = cursor.fetchall()
        cursor.execute(sql_item_count.substitute(table=table))
        item_count = cursor.fetchall()
        cursor.execute(sql_date_count.substitute(table=table))
        date_count = cursor.fetchall()
        print(f"\n{table} details:\nRecords:       {record_count[0][0]}")
        print(f"Column count:  {column_count[0][0]}")
        print(f"Customer IDs:  {id_count[0][0]}")
        print(f"Items:         {item_count[0][0]}")
        print(f"Shop dates:    {date_count[0][0]}")
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

    #~ Boots cards come as UTF16 TSV: convert to UTF8 CSV
    with codecs.open(args.input.name, "r", encoding="utf-16") as input_file:
        card_file_contents = input_file.read()
        utf8_card_file_contents = card_file_contents.replace("\t", ",")
        with codecs.open("UTF8" + args.input.name, "w", encoding="utf-8") as output_file:
            output_file.write(utf8_card_file_contents)

    create_table(
        args.table,
        connection,
        cursor)

    import_csv_to_pg_table(
        args.db,
        args.input,
        args.table,
        connection,
        cursor)

    db_details(
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
