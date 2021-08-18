import sys
import os
import re
import argparse
import csv
import pandas as pd
import sqlite3

import csv2sql

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


def query_builder(
    date="",
    customer=""):
    # since="", #! TO DO!
    # until="",

    """
    Builds the SQL query statements from supplied args.

    ARGS:   Database details (db, table, cursor, connection)
            Date in the format YYYYMMDD, for example 20180426
            For data ranges, provide two dates (either order will work)
            For customers, number(s), ID in format CUST0123456789
    """

    queries_list = []

    if customer: #~ construct the customer query SQL
        query_customer = f"""CUST_CODE = \"{customer}\""""
        queries_list.append(query_customer)

    if date: #~ construct the date query SQL
        if len(date) == 1:
            query_date = f"""SHOP_DATE = \"{date[0]}\""""
        if len(date) == 2:
            query_date = f"""SHOP_DATE >= \"{min(date)}\" AND SHOP_DATE <= \"{max(date)}\""""
        if len(date) > 2:
            print(f"\n!!! Please provide only two dates.")
            sys.exit(1)
        queries_list.append(query_date)

    return queries_list


def query_runner(
    db,
    table,
    cursor,
    connection,
    queries):

    conjunction = ""
    if len(queries) > 1:
        conjunction = "AND"
    print(queries)
    try:
        cursor.execute(f"""
            SELECT * FROM "{table}"
            WHERE {queries[0]}
            {conjunction} {queries[1]};""")

        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(f"!!! There was a problem: {e}")
        csv2sql.examine_db(cursor, db)


def main():

    parser, args = args_setup()

    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    if not os.path.exists(args.db):
        print(f"!!! The database {args.db} doesn't seem to exist here.")
        sys.exit(1)

    #~ connect!
    connection, cursor = sqlite_connect(args.db)

    queries = query_builder(
        date=args.date,
        customer=args.cust)

    query_runner(
        args.db,
        args.table,
        cursor,
        connection,
        queries)


if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        print("OK, stopping.")

