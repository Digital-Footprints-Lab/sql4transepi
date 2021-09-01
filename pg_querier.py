import sys
import os
import signal
import re
from string import Template
import argparse
import csv
import pandas as pd
import psycopg2


def args_setup():

    parser = argparse.ArgumentParser(
        description="PostgreSQL DB Querier",
        epilog="Example: python pg_querier.py -d database1.db -t table1 --cust CUST001")
    parser.add_argument(
        "--details", action="store_true",
        help="Provide more details information on DB interactions and outputs.")
    parser.add_argument(
        "-c", "--count", action="store_true",
        help="Provide total record counts rather than actual record output.")
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
        "--hour", action="store",
        help="Shop hour to query (24 hour, 2 digits). Format: HH")
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

    args = parser.parse_args()

    return parser, args


def signal_handler(sig, frame):

    """Handle interrupt signals, eg ctrl-c (and other kill signals)."""

    print(f"\nJust a second while I try to exit gracefully...")

    try:
        connection.close()
    except Exception as e:
        print(e)

    sys.exit(1)


def all_records_from_product(
    product,
    record_type,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE PROD_CODE = '$product';""")

    try:
        cursor.execute(sql.substitute(record_type=record_type, table=table, product=product))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)

#~ time queries =========================================
def all_records_from_hour(
    hour,
    record_type,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE SHOP_HOUR = '$hour';""")

    try:
        cursor.execute(sql.substitute(record_type=record_type, table=table, hour=hour))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def all_records_from_date(
    date,
    record_type,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE SHOP_DATE = '$date';""")

    try:
        cursor.execute(sql.substitute(record_type=record_type, table=table, date=date))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def all_records_from_week(
    week,
    record_type,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE SHOP_WEEK = '$week';""")

    try:
        cursor.execute(sql.substitute(record_type=record_type, table=table, week=week))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def all_records_from_weekday(
    weekday,
    record_type,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE SHOP_WEEKDAY = '$weekday';""")

    try:
        cursor.execute(sql.substitute(record_type=record_type, table=table, weekday=weekday))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


#~ CUSTOMER queries =========================
def customer_records_all(
    customer,
    record_type,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE CUST_CODE = '$customer';""")

    try:
        cursor.execute(sql.substitute(record_type=record_type, table=table, customer=customer))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def customer_records_from_date(
    customer,
    date,
    record_type,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE SHOP_DATE = '$date'
        AND CUST_CODE = '$customer';""")

    try:
        cursor.execute(sql.substitute(record_type=record_type, table=table, customer=customer, date=date))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def customer_records_from_week(
    customer,
    week,
    record_type,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE CUST_CODE = '$customer'
        AND SHOP_WEEK = '$week';""")

    try:
        cursor.execute(sql.substitute(record_type=record_type, table=table, customer=customer, week=week))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def customer_records_from_weekday(
    customer,
    weekday,
    record_type,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE CUST_CODE = '$customer'
        AND SHOP_WEEKDAY = '$weekday';""")

    try:
        cursor.execute(sql.substitute(record_type=record_type, table=table, customer=customer, weekday=weekday))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


#~ BASKET QUERIES ==========================
def basket_all_records(
    basket,
    record_type,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE BASKET_ID = '$basket';""")

    try:
        cursor.execute(sql.substitute(record_type=record_type, table=table, basket=basket))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def basket_spend(
    basket,
    record_type,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT SUM(SPEND) FROM $table
        WHERE BASKET_ID = '$basket';""")

    try:
        cursor.execute(sql.substitute(record_type=record_type, table=table, basket=basket))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


#~ SPEND QUERIES ===========================
#~ the SPEND column is already (product price * QUANTITY)
#~ (individual product price is NOT a column)
def spend_by_customer_total(
    customer,
    record_type,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT SUM(SPEND) FROM $table
        WHERE CUST_CODE = '$customer';""")

    try:
        cursor.execute(sql.substitute(record_type=record_type, table=table, customer=customer))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def spend_by_customer_on_date(
    customer,
    date,
    record_type,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT SUM(SPEND) FROM $table
        WHERE CUST_CODE = '$customer'
        AND SHOP_DATE = '$date';""")

    try:
        cursor.execute(sql.substitute(record_type=record_type, table=table, customer=customer, date=date))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def spend_by_customer_on_week(
    customer,
    week,
    record_type,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT SUM(SPEND) FROM $table
        WHERE CUST_CODE = '$customer'
        AND SHOP_WEEK = '$week';""")

    try:
        cursor.execute(sql.substitute(record_type=record_type, table=table, customer=customer, week=week))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


def spend_by_customer_on_weekday(
    customer,
    weekday,
    record_type,
    table,
    cursor,
    connection):

    sql = Template("""
        SELECT SUM(SPEND) FROM $table
        WHERE CUST_CODE = '$customer'
        AND SHOP_WEEKDAY = '$weekday';""")

    try:
        cursor.execute(sql.substitute(record_type=record_type, table=table, customer=customer, weekday=weekday))
        result = cursor.fetchall()
        print(result)
    except Exception as e:
        print(e)


#~ GENERAL STATUS QUERY =====================
def db_details(
    db,
    table,
    cursor,
    connection):

    sql_record_count = Template("""
        SELECT COUNT(*)
        FROM $table;""")
    sql_column_count = Template("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name='$table';""")
    sql_cust_count = Template("""
        SELECT COUNT (DISTINCT CUST_CODE) FROM $table;""")
    sql_basket_count = Template("""
        SELECT COUNT (DISTINCT BASKET_ID) FROM $table;""")
    sql_date_count = Template("""
        SELECT COUNT (DISTINCT SHOP_DATE) FROM $table;""")

    try:
        cursor.execute(sql_record_count.substitute(table=table))
        record_count = cursor.fetchall()
        cursor.execute(sql_column_count.substitute(table=table))
        column_count = cursor.fetchall()
        cursor.execute(sql_cust_count.substitute(table=table))
        cust_count = cursor.fetchall()
        cursor.execute(sql_basket_count.substitute(table=table))
        basket_count = cursor.fetchall()
        cursor.execute(sql_date_count.substitute(table=table))
        date_count = cursor.fetchall()
        print(f"\n{table} details:\nRecords:     {record_count[0][0]}")
        print(f"Columns:     {column_count[0][0]}")
        print(f"Customers:   {cust_count[0][0]}")
        print(f"Baskets:     {basket_count[0][0]}")
        print(f"Shop dates:  {date_count[0][0]}")
    except Exception as e:
        print(e)


#~ main =================================
def main():

    # signal.signal(signal.SIGINT, signal_handler)
    # parser, args = args_setup()

    #~ connect to pgsql - if no DB, see exception.
    try:
        connection = psycopg2.connect(
            database=args.db,
            user="at9362",
            password="password",
            host="127.0.0.1",
            port="5432")
        cursor = connection.cursor()
    except psycopg2.OperationalError as e:
        print(f"The database {args.db} doesn't seem to exist.")
        print(f"See the script csv2pg.py if you would like to create one from a CSV file.")
        sys.exit(1)

    if args.details:
        print(f"DB connection details:\n", connection.get_dsn_parameters())
        db_details(
            args.db,
            args.table,
            cursor,
            connection)

    #~ if args.count is not included, SELECTs will be for records,
    #~ flip this to COUNT if we only want counts.
    record_type = "*"
    if args.count:
        record_type = "COUNT(*)"

#~ I know this is kinda crazy - will make an arg handler later :)
#~ three args =======================
    if args.cust and args.date and args.spend:
        spend_by_customer_on_date(
            args.cust,
            args.date,
            record_type,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.cust and args.week and args.spend:
        spend_by_customer_on_week(
            args.cust,
            args.week,
            record_type,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.cust and args.weekday and args.spend:
        spend_by_customer_on_weekday(
            args.cust,
            args.weekday,
            record_type,
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
            record_type,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.cust and args.week:
        customer_records_from_week(
            args.cust,
            args.week,
            record_type,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.cust and args.weekday:
        customer_records_from_weekday(
            args.cust,
            args.weekday,
            record_type,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.basket and args.spend:
        basket_spend(
            args.basket,
            record_type,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.cust and args.spend:
        spend_by_customer_total(
            args.cust,
            record_type,
            args.table,
            cursor,
            connection)
        connection.close()
        return

#~ one arg ===========================
    if args.cust:
        customer_records_all(
            args.cust,
            record_type,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.prod:
        all_records_from_product(
            args.prod,
            record_type,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.hour:
        all_records_from_hour(
            args.hour,
            record_type,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.date:
        all_records_from_date(
            args.date,
            record_type,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.week:
        all_records_from_week(
            args.week,
            record_type,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.weekday:
        all_records_from_weekday(
            args.weekday,
            record_type,
            args.table,
            cursor,
            connection)
        connection.close()
        return

    if args.basket:
        basket_all_records(
            args.basket,
            record_type,
            args.table,
            cursor,
            connection)
        connection.close()
        return


if __name__ == "__main__":

    signal.signal(signal.SIGINT, signal_handler)

    parser, args = args_setup()

    main()

