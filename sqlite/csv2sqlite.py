import sys
import os
import re
import argparse
import csv
import pandas as pd
import sqlite3
from sqlite3 import Error


def args_setup():

    parser = argparse.ArgumentParser(
        description="SQLite DB Importer and Updater",
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


def sqlite_connect(db):

    try:
        if not os.path.exists(db):
            print(f"{db} doesn't exist here: creating DB...")
        connection = sqlite3.connect(db)
        #~ c is the cursor object: executes SQL on DB table
        cursor = connection.cursor()
        print(f"Connected to DB '{db}'...")

    except Exception as e:
        print(f"\n!!! Problem connecting to DB: {e}")

    return connection, cursor


def csv_to_sqlite_table(
    db,
    table,
    csv_file,
    cursor,
    connection):

    """
    Imports a CSV file into SQLite table. If the table already
    exists, the CSV is added, with any duplicates ignored.
    "Duplicate" means a record where every field is identical.

    ARGS: sqlite cursor + connection,
          name of table to index,
          name of incoming csv file,
          name to apply to new table.

    RETS: nothing, commits changes to SQLite DB.
    """

    if not re.match("^[a-zA-Z0-9]+$", table):
        print("\n!!! Table names can only include standard characters. Please retry.")
        connection.close()
        sys.exit(1)
    if table[0].isnumeric():
        print("\n!!! Table names cannot start with a number. Please retry.")
        connection.close()
        sys.exit(1)

    #~ check if table already exists
    cursor.execute(f"""
        SELECT name FROM sqlite_master
        WHERE type="table" AND name="{table}";""")

    if cursor.fetchone() == None: #~ table doesn't exit yet
        print(f"\nCreating new table '{table}' in DB '{db}'...")
        try:
            # cursor.execute(f""" #! COPY is not a valid SQLite command!! ffs.
            #     COPY {table} FROM '/Users/at9362/Code/sql4transepi/first100records.csv'
            #     DELIMITER ','
            #     CSV HEADER;""") #~ so create table from csv
            pd.read_csv(csv_file).to_sql(
                table,
                connection,
                if_exists="fail",
                index=False)
            print(f"OK, import successful.")
        except Exception as e:
            print(f"\n!!! {e} \n!!! {csv_file.name} not imported.")

    #! every solution to this seems to require listing all of the columns.
    #! which is clearly ridiculous. leaving for now. the below is
    #! import to temp table (dangerous anyway), then merge tables (not straightforward afaics)
    #! why is this so difficult!?!?!!?
    else: #~ table exists,
        print(f"\nTable '{table}' exists, appending records... TODO")
        # try:                 #~ put csv into temporary table
        #     table_incoming = table + "_incoming"
        #     pd.read_csv(csv_file).to_sql(
        #     table_incoming,
        #     connection,
        #     if_exists="fail",
        #     index=False)

        #     with open(csv_file.name, "r") as infile:
        #         print("fix me!")
        #         pass #! fix me!
        # except Exception as e:
        #     print(f"\n!!! {e} \n!!! {csv_file.name} not appended.")

    connection.commit()


def create_index(
    cursor,
    connection,
    table_name,
    index_name,
    index_column):

    """
    ARGS: sqlite cursor + connection,
          name of table to index,
          name you are giving to your new index,
          which column to be indexed

    RETS: nothing, commits changes to SQLite DB.
    """

    try:
        cursor.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS
            {index_name} ON
            {table_name}({index_column});""")

        connection.commit()

    except Exception as e:
        print("!!! Index creation failed", e)


def examine_db(cursor, db):

    """
    Basic summary of the DB status.
    Prints the number and size of each table.
    """

    try:
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table';")
        tables = [x[0] for x in cursor.fetchall()]
        print(f"\nDatabase '{db}' currently contains {len(tables)} tables.")
        for t in tables:
            cursor.execute(f"SELECT max(rowid) FROM {t};")
            row_count = cursor.fetchone()[0]
            cursor.execute(f"PRAGMA table_info({t});")
            column_count = len(cursor.fetchall())
            print(f"{row_count} records across {column_count} columns in table '{t}'.")
    except Exception as e:
        print(e)


def main():

    parser, args = args_setup()

    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    #~ connect!
    connection, cursor = sqlite_connect(args.db)

    #~ bring product csv into sqlite table
    csv_to_sqlite_table(
        args.db,
        args.table,
        args.file,
        cursor,
        connection)

    # #~ index on productid column
    # create_index(cursor,
    #             connection,
    #             product_table,
    #             product_index,
    #             "productid")

    examine_db(cursor, args.db)

    try:
        connection.close()
        print("\nConnection closed.")
    except Exception as e:
        print(f"\n!!! Problem closing connection: {e}")


if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        print("OK, stopping.")

