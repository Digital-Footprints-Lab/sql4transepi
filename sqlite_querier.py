import sys
import os
import re
import argparse
import csv
import pandas as pd
import sqlite3

#~ my local imports
import csv2sql

#! query on basket total / per customer / date

#! query on what dates particular product codes / total counts summaries / per customer.
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
        "--cust", nargs="+", action="store",
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

    """
    Builds the SQL query statements from supplied args.

    ARGS:   Database details (db, table, cursor, connection)
            Date in the format YYYYMMDD, for example 20180426
            For data ranges, provide two dates (either order will work)
            For customers, number(s), ID in format CUST0123456789
    """

    queries = []

    if customer: #~ construct the customer query SQL
        query_customer = f"""CUST_CODE = \"{customer[0]}\""""
        if len(customer) > 1:
            for cust in customer[1:]:
                query_customer = query_customer + f""" OR CUST_CODE = \"{cust}\""""
        queries.append(query_customer)

    if date: #~ construct the date query SQL
        if len(date) == 1:
            query_date = f"""SHOP_DATE = \"{date[0]}\""""
        if len(date) == 2:
            query_date = f"""SHOP_DATE >= \"{min(date)}\" AND SHOP_DATE <= \"{max(date)}\""""
        if len(date) > 2:
            print(f"\n!!! Please provide only two dates.")
            sys.exit(1)
        queries.append(query_date)

    if len(queries) == 1:
        query_string = (queries[0])
    else: #~ concatenate multiple queries with "AND" between
        query_string = queries[0]
        for query in queries[1:]:
            query_string = (query_string + " AND " + query)

    return query_string


def query_runner(
    db,
    table,
    cursor,
    connection,
    query_string):

    """
    Runs the query built by the query builder, straight outputs
    the matches to the terminal for now.

    ARGS:   SQL stuff (db, table, cursor, connection)
            A string of SQL formatted queries/query, as made by
            query_builder())
    """

    try:
        cursor.execute(f"""
            SELECT * FROM {table}
            WHERE {query_string};""")

        result = cursor.fetchall()
        print(result)

    except Exception as e:
        print(f"!!! There was a problem: {e}")
        csv2sql.examine_db(cursor, db)

def main():

    parser, args = args_setup()

    if len(sys.argv) < 6:
        parser.print_help()
        sys.exit(1)

    if not os.path.exists(args.db):
        print(f"!!! The database {args.db} doesn't seem to exist here.")
        sys.exit(1)

    #~ connect!
    connection, cursor = sqlite_connect(args.db)

    query_string = query_builder(
        date=args.date,
        customer=args.cust)

    query_runner(
        args.db,
        args.table,
        cursor,
        connection,
        query_string)


if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        print("OK, stopping.")

