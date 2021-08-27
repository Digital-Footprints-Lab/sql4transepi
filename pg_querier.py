import sys
import os
import re
from string import Template
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
    parser.add_argument(
        "--basket", action="store",
        help="Basket ID. Format: 123450123456789")
    parser.add_argument(
        "--spend", action="store_true",
        help="Total spend for customer, all time.")
    # parser.add_argument(
    #     "--datecust", action="store",
    #     help="Results from a single customer on a single date.")

    args = parser.parse_args()

    return parser, args

#! date summary
#! week summary
#! basket summary
#! customer summary
#! combined time + cust
#! product summaries

def all_records_from_customer(
    customer,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT * FROM $table
        WHERE CUST_CODE = '$customer';""")

    try:
        cursor.execute(sql.substitute(table=table, customer=customer))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def all_records_from_product(
    product,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT * FROM $table
        WHERE PROD_CODE = '$product';""")

    try:
        cursor.execute(sql.substitute(table=table, product=product))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def all_records_from_date(
    date,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT * FROM $table
        WHERE SHOP_DATE = '$date';""")

    try:
        cursor.execute(sql.substitute(table=table, date=date))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def all_records_from_week(
    week,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT * FROM $table
        WHERE SHOP_WEEK = '$week';""")

    try:
        cursor.execute(sql.substitute(table=table, week=week))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def all_records_from_weekday(
    weekday,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT * FROM $table
        WHERE SHOP_WEEKDAY = '$weekday';""")

    try:
        cursor.execute(sql.substitute(table=table, weekday=weekday))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


#~ CUSTOMER queries =========================
def customer_records_from_date(
    customer,
    date,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT * FROM $table
        WHERE SHOP_DATE = '$date'
        AND CUST_CODE = '$customer';""")

    try:
        cursor.execute(sql.substitute(table=table, customer=customer, date=date))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def customer_records_from_week(
    customer,
    week,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT * FROM $table
        WHERE CUST_CODE = '$customer'
        AND SHOP_WEEK = '$week';""")

    try:
        cursor.execute(sql.substitute(table=table, customer=customer, week=week))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def customer_records_from_weekday(
    customer,
    weekday,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT * FROM $table
        WHERE CUST_CODE = '$customer'
        AND SHOP_WEEKDAY = '$weekday';""")

    try:
        cursor.execute(sql.substitute(table=table, customer=customer, weekday=weekday))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


#~ BASKET QUERIES ==========================
def basket_all_records(
    basket,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT * FROM $table
        WHERE BASKET_ID = '$basket';""")

    try:
        cursor.execute(sql.substitute(table=table, basket=basket))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def basket_spend(
    basket,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT SUM(SPEND) FROM $table
        WHERE BASKET_ID = '$basket';""")

    try:
        cursor.execute(sql.substitute(table=table, basket=basket))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


#~ SPEND QUERIES ===========================
def spend_by_customer_total(
    customer,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT SUM(SPEND) FROM $table
        WHERE CUST_CODE = '$customer';""")

    try:
        cursor.execute(sql.substitute(table=table, customer=customer))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def spend_by_customer_on_date(
    customer,
    date,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT SUM(SPEND) FROM $table
        WHERE CUST_CODE = '$customer'
        AND SHOP_DATE = '$date';""")

    try:
        cursor.execute(sql.substitute(table=table, customer=customer, date=date))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def spend_by_customer_on_week(
    customer,
    week,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT SUM(SPEND) FROM $table
        WHERE CUST_CODE = '$customer'
        AND SHOP_WEEK = '$week';""")

    try:
        cursor.execute(sql.substitute(table=table, customer=customer, week=week))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def spend_by_customer_on_weekday(
    customer,
    weekday,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT SUM(SPEND) FROM $table
        WHERE CUST_CODE = '$customer'
        AND SHOP_WEEKDAY = '$weekday';""")

    try:
        cursor.execute(sql.substitute(table=table, customer=customer, weekday=weekday))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


#~ ==================================
def count_customer_records():
    pass


def count_product_records():
    pass


def count_date_records():
    pass


#~ main =================================
def main():

    parser, args = args_setup()

    #~ connect to pgsql - if no DB, see exception.
    try:
        connection = psycopg2.connect(
            database=args.db,
            user="at9362",
            password="password",
            host="127.0.0.1",
            port="5432")
    except psycopg2.OperationalError as e:
        print(f"The database {args.db} doesn't seem to exist.")
        print(f"See the script csv2pg.py if you would like to create one from a CSV file.")
        sys.exit(1)

    cursor = connection.cursor()

#~ I know this is kinda crazy - will make an arg handler later :)
#~ three args =======================
    if args.cust and args.date and args.spend:
        spend_by_customer_on_date(
            args.cust,
            args.date,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.cust and args.week and args.spend:
        spend_by_customer_on_week(
            args.cust,
            args.week,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.cust and args.weekday and args.spend:
        spend_by_customer_on_weekday(
            args.cust,
            args.weekday,
            args.table,
            cursor,
            connection)
        connection.close()
        return

#~ two args ==========================
    if args.cust and args.date:
        customer_records_from_date(
            args.cust,
            args.date,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.cust and args.week:
        customer_records_from_week(
            args.cust,
            args.week,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.cust and args.weekday:
        customer_records_from_weekday(
            args.cust,
            args.weekday,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.basket and args.spend:
        basket_spend(
            args.basket,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.cust and args.spend:
        spend_by_customer_total(
            args.cust,
            args.table,
            cursor,
            connection)
        connection.close()
        return

#~ one arg ===========================
    if args.cust:
        all_records_from_customer(
            args.cust,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.prod:
        all_records_from_product(
            args.prod,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.date:
        all_records_from_date(
            args.date,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.week:
        all_records_from_week(
            args.week,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.weekday:
        all_records_from_weekday(
            args.weekday,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.basket:
        basket_all_records(
            args.basket,
            args.table,
            cursor,
            connection)
        connection.close()
        return


if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        print("OK, stopping.")
