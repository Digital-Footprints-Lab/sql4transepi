import sys
import os
import re
import argparse
import csv
import pandas as pd
import psycopg2


def args_setup():

    parser = argparse.ArgumentParser(
        description="PostgreSQL DB Querier",
        epilog="Example: python querier.py -d database1.db --cust CUST001")
    parser.add_argument(
        "-d", "--db", action="store", required=True,
        help="The name of the DB to query.")
    parser.add_argument(
        "-t", "--table", action="store", required=True,
        help="The name of the table to query.")
    parser.add_argument(
        "--cust", "--customer", action="store",
        help="Customer code to query. Format: CUST0123456789")
    parser.add_argument(
        "--prod", "--product", action="store",
        help="Product code to query. Format: PRD0123456")
    parser.add_argument(
        "--date", action="store",
        help="Shop date to query. Format: YYYYMMDD")
    parser.add_argument(
        "--week", action="store",
        help="Shop week (of year) to query. Format: YYYYNN")
    parser.add_argument(
        "--weekday", action="store",
        help="Shop weekday (1-7) to query. Format: N")

    args = parser.parse_args()

    return parser, args

#! query on basket total / per customer / date
#! query on what dates particular product codes / total counts summaries / per customer.
#! date summary
#! week summary
#! basket summary
#! customer summary
#! combined time + cust
#! product summaries
#! port to prepared statements

#! here >>>
def total_spend_by_customer_for_week(customer, week):

    """

    """

    statement = "SELECT % blah"
    #~ bind customer and week into statement
    #!
    #~ execute statement
    #!cursor
    #~ collect results from cursor
    #!
    #~ return!


def all_records_from_customer(
    customer,
    table,
    cursor,
    connection):

    sql = f""" #! turn this to prepared statement!
        SELECT * FROM {table}
        WHERE CUST_CODE = '{customer}';"""

    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def all_records_from_product(
    product,
    table,
    cursor,
    connection):

    sql = f"""
        SELECT * FROM {table}
        WHERE PROD_CODE = '{product}';"""

    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def all_records_from_date(
    date,
    table,
    cursor,
    connection):

    sql = f"""
        SELECT * FROM {table}
        WHERE SHOP_DATE = '{date}';"""

    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def all_records_from_week(
    week,
    table,
    cursor,
    connection):

    sql = f"""
        SELECT * FROM {table}
        WHERE SHOP_WEEK = '{week}';"""

    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def all_records_from_weekday(
    weekday,
    table,
    cursor,
    connection):

    sql = f"""
        SELECT * FROM {table}
        WHERE SHOP_WEEKDAY = '{weekday}';"""

    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def count_customer_records():
    pass


def count_product_records():
    pass


def count_date_records():
    pass







def main():

    parser, args = args_setup()

    #~ the database can be made on the command line with
    #~ createdb [dbname]
    connection = psycopg2.connect(
        database=args.db,
        user="at9362",
        password="password",
        host="127.0.0.1",
        port="5432")

    #~ Create a cursor object using the cursor() method
    cursor = connection.cursor()

    if args.cust:
        all_records_from_customer(
            args.cust,
            args.table,
            cursor,
            connection)

    if args.prod:
        all_records_from_product(
            args.prod,
            args.table,
            cursor,
            connection)

    if args.date:
        all_records_from_date(
            args.date,
            args.table,
            cursor,
            connection)

    if args.week:
        all_records_from_week(
            args.week,
            args.table,
            cursor,
            connection)

    if args.weekday:
        all_records_from_weekday(
            args.weekday,
            args.table,
            cursor,
            connection)



if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        print("OK, stopping.")
