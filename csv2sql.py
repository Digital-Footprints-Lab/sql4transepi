import sys
import argparse
import csv
import pandas as pd
import sqlite3
from sqlite3 import Error


def args_setup():

    parser = argparse.ArgumentParser(
        description="SQLite DB Importer and Updater",
        epilog="Example: python csv2sql.py -i items.csv -d database1.db -t items")
    parser.add_argument(
        "-f", "--file", type=argparse.FileType("r"), default=sys.stdin,
        metavar="PATH", required=True,
        help="CSV file to import.")
    parser.add_argument( #! redundant
        "-a", "--append", action="store_true",
        help="Append to existing table.")
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


def sqlite_connect(db_name):

    #~ connect to our working DB
    connection = sqlite3.connect(db_name)
    #~ c is the cursor object: executes SQL on DB table
    cursor = connection.cursor()

    return connection, cursor


def csv_to_sqlite_table(
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

    #~ check if table already exists
    cursor.execute(f"""
        SELECT name FROM sqlite_master
        WHERE type="table" AND name="{table}";""")

    if cursor.fetchone() == None: #~ table doesn't exit yet
        print(f"\nCreating new table '{table}' in DB '{db}'...")
        try:                      #~ so create table from csv
            pd.read_csv(csv).to_sql(
                table,
                connection,
                if_exists="fail",
                index=False)
            print(f"OK, import successful.")
        except Exception as e:
            print(f"\n!!! {e} \n!!! {csv.name} not imported.")

    else: #~ table exists, do an INSERT
        print(f"\nTable '{table}' exists, appending records...")
        try:
            cursor.execute(f"""
                INSERT INTO {table}
                (SHOP_WEEK,SHOP_DATE,SHOP_WEEKDAY,SHOP_HOUR,QUANTITY,SPEND,PROD_CODE,PROD_CODE_10,PROD_CODE_20,PROD_CODE_30,PROD_CODE_40,CUST_CODE,CUST_PRICE_SENSITIVITY,CUST_LIFESTAGE,BASKET_ID,BASKET_SIZE,BASKET_PRICE_SENSITIVITY,BASKET_TYPE,BASKET_DOMINANT_MISSION,STORE_CODE,STORE_FORMAT,STORE_REGION)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""")
        except Exception as e:
            print(f"\n!!! {e} \n!!! {csv.name} not appended.")
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


#! fix me
# def append_csv_to_table(
#     db,
#     cursor,
#     connection,
#     csv,
#     table):

#     """
#     Reads the incoming csv into a dataframe,
#     then iterates through each row, appending the relevant fields.
#     If there are duplicate on unique index column, skip and do nothing.

#     ARGS: sqlite cursor + connection,
#           input csv file to be added,
#           name of the table to be added to

#     RETS: nothing, commits changes to the SQLite DB.
#     """

#     print(f"\nAppending {csv.name} to table '{table}' in DB '{db}'...")

#     #~ check if table actually exists
#     try:
#         cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table';")
#         tables = [x[0] for x in cursor.fetchall()]
#         if not table in tables:
#             print(f"\n!!! Table '{table}' doesn't exist in database '{db}'.")
#             print(f"Remove the append flag to create a new table instead.")
#             sys.exit(1)
#     except Exception as e:
#         print(e)

#     incoming_df = pd.read_csv(csv)
#     # try:
#     for _, row in incoming_df.iterrows():
#         cursor.execute(
#             f"""INSERT INTO {table}
#                 (productid, name, PDP_productPrice)
#                 VALUES(
#                 "{row["productid"]}",
#                 "{row["name"]}",
#                 "{row["PDP_productPrice"]}")
#                 ON CONFLICT DO NOTHING;""")
#     # pd.read_csv(csv).to_sql( #! pandas cannot check for dups
#     #     table,
#     #     connection,
#     #     if_exists="append",
#     #     index=False)
#     # print(f"OK, append successful.")
#     # except Exception as e:
#     #     print(f"\n!!! {e} {csv.name} not appended.")

#     connection.commit()


def examine_db(cursor, table_name, db):

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

    file = args.file
    db = args.db
    table = args.table
    if table.isnumeric(): #! this doesn't catch decimals
        print("\n!!! Tables cannot be named as a number. Please try again.")
        sys.exit(1)

    #~ connect!
    connection, cursor = sqlite_connect(db)

    if args.append:

        append_csv_to_table(
            db,
            cursor,
            connection,
            file,
            table)

    else:
        #~ bring product csv into sqlite table
        csv_to_sqlite_table(
            db,
            cursor,
            connection,
            file,
            table)

    # #~ adding new products to table to test duplicate prevention
    # add_csv_lines_to_table(cursor, connection, more_csv, product_table)

    # #~ index on productid column
    # create_index(cursor,
    #             connection,
    #             product_table,
    #             product_index,
    #             "productid")

    examine_db(cursor, table, db)

    #~ close connection to DB
    connection.close()

if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        print("OK, stopping.")

