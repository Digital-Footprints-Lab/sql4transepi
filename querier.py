import sys
import argparse
import csv
import pandas as pd
import sqlite3

"""Eventually, we might want to use
pypika SQL query builder package, although right now
best leave the full-fledged packages alone
until we need them / you've learned the basics"""


def args_setup():

    parser = argparse.ArgumentParser(
        description="SQLite DB Querier",
        epilog="Example: python querier.py -d database1.db -c CUST001")
    parser.add_argument(
        "-d", "--db", action="store", required=True,
        help="The name of the DB to query.")
    parser.add_argument(
        "-t", "--table", action="store", required=True,
        help="The name of the table to query.")
    parser.add_argument(
        "--date", nargs="+", action="store",
        help="Shop date(range) to query, provide one or two dates. Format: YYYYMMDD")
    parser.add_argument(
        "--cust", action="store",
        help="Customer code to query. Format: CUST0123456789")

    args = parser.parse_args()

    return parser, args


def sqlite_connect(db_name):

    #~ connect to our working DB
    connection = sqlite3.connect(db_name)
    #~ c is the cursor object: executes SQL on DB table
    cursor = connection.cursor()

    return connection, cursor


def customer_query(
    db,
    table,
    cursor,
    connection,
    customer):

    """
    Carries out a simple customer query
    """

    cursor.execute(f"""
        SELECT * FROM "{table}"
        WHERE CUST_CODE = "{customer}";""")
    result = cursor.fetchall()
    print(result)


def date_query(
    db,
    table,
    cursor,
    connection,
    date):

    """
    Carries out a simple date query
    """

    if len(date) == 1:
        cursor.execute(f"""
            SELECT * FROM "{table}"
            WHERE SHOP_DATE = "{date[0]}";""")

    if len(date) == 2:
        cursor.execute(f"""
            SELECT * FROM "{table}"
            WHERE SHOP_DATE >= "{min(date)}" AND SHOP_DATE <= "{max(date)}";""")

    if len(date) > 2:
        print(f"\n!!! Please provide only two dates. Stopping.")
        sys.exit(1)

    result = cursor.fetchall()
    print(result)


def main():

    parser, args = args_setup()

    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    db = args.db
    table = args.table
    customer = args.cust
    date = args.date

    #~ connect!
    connection, cursor = sqlite_connect(db)

    if args.cust:
        customer_query(db, table, cursor, connection, customer)

    if args.date:
        date_query(db, table, cursor, connection, date)



if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        print("OK, stopping.")

