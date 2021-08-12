import sys
import argparse
import csv
import pandas as pd
import sqlite3
from sqlite3 import Error


def args_setup():

    parser = argparse.ArgumentParser(
        description="SQLite DB Importer and Updater",
        epilog="Example: python csv2sql.py -i monday.csv -d database1.db -t monday")
    parser.add_argument(
        "-i", "--infile", type=argparse.FileType("r"), default=sys.stdin,
        metavar="PATH",
        help="CSV file to import.")
    parser.add_argument(
        "-a", "--append", action="store_true",
        help="Append to existing table.")
    parser.add_argument(
        "-d", "--db", action="store",
        help="The name of the DB to work with.")
    parser.add_argument(
        "-t", "--table", type=str, action="store",
        help="The name of the table to work with.")

    args = parser.parse_args()

    return parser, args


def sqlite_connect(db_name):

    #~ connect to our working DB
    connection = sqlite3.connect(db_name)
    #~ c is the cursor object: executes SQL on DB table
    cursor = connection.cursor()

    return connection, cursor


def csv_to_new_sqlite_table(
    db,
    cursor,
    connection,
    csv,
    table):

    """
    ARGS: sqlite cursor + connection,
          name of table to index,
          name of incoming csv file,
          name to apply to new table.

    RETS: nothing, commits changes to SQLite DB.
    """

    print(f"\nImporting {csv.name} to table '{table}' in DB '{db}'...")

    try:
        pd.read_csv(csv).to_sql(
            table,
            connection,
            if_exists="fail",
            index=False)
        print(f"OK, import successful.")

    except Exception as e:
        print(f"\n{e} {csv.name} not imported.")
        print("Use the flag '-a' if you want to append to an existing table.")
        print("Example: \npython csv2sql.py -i monday.csv -d database1.db -t table -a")

    connection.commit()

    table_count(cursor, table)

    return


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
        cursor.execute(
            f"""CREATE UNIQUE INDEX IF NOT EXISTS
                {index_name} ON
                {table_name}({index_column});""")

        connection.commit()

    except Exception as e:
        print("Index creation failed", e)


def append_csv_to_table(
    db,
    cursor,
    connection,
    csv,
    table):

    """
    Reads the incoming csv into a dataframe,
    then iterates through each row, inserting the relevant fields.
    If there are duplicate on unique index column, skip and do nothing.

    ARGS: sqlite cursor + connection,
          input csv file to be added,
          name of the table to be added to

    RETS: nothing, commits changes to the SQLite DB.
    """
    #! TODODO: just be the same shape as the existing table.
    incoming_df = pd.read_csv(csv)

    #! this cannot deal with unclean data. sql hates quotes and brackets. todo.
    #! [although cleaning should be elsewhere. raw dirty, DB clean.]
    for _, row in incoming_df.iterrows():
        cursor.execute(
            f"""INSERT INTO {table}
                (productid, name, PDP_productPrice)
                VALUES(
                "{row["productid"]}",
                "{row["name"]}",
                "{row["PDP_productPrice"]}")
                ON CONFLICT DO NOTHING;""")

    connection.commit()


def table_count(cursor, table_name):

    try:
        cursor.execute(f"SELECT max(rowid) FROM {table_name};")
        row_count = cursor.fetchone()[0]
        cursor.execute(f"PRAGMA table_info({table_name});")
        column_count = len(cursor.fetchall())
        print(f"There are currently {row_count} records across {column_count} columns in '{table_name}'.")
    except Exception as e:
        print(e)


def main():

    parser, args = args_setup()

    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(0)

    infile = args.infile
    db = args.db
    table = args.table

    #~ connect!
    connection, cursor = sqlite_connect(db)

    if args.append:

        append_csv_to_table(
            db,
            cursor,
            connection,
            infile,
            table)

    else:
        #~ bring product csv into sqlite table
        csv_to_new_sqlite_table(
            db,
            cursor,
            connection,
            infile,
            table)

    # #~ adding new products to table to test duplicate prevention
    # add_csv_lines_to_table(cursor, connection, more_csv, product_table)

    # #~ index on productid column
    # create_index(cursor,
    #             connection,
    #             product_table,
    #             product_index,
    #             "productid")

    #~ close connection to DB
    connection.close()

if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        print("OK, stopping.")

