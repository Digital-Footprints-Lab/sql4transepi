
#~ Standard library imports
import sys
import os
import traceback
import re
from string import Template
import argparse
import csv

#~ 3rd party imports
import pandas as pd
import psycopg2

#~ local imports
import db_config


def args_setup():

    parser = argparse.ArgumentParser(
        description="PostgreSQL DB Querier",
        epilog="Example: python pg_querier.py --customer CUST001 --date 20180621 --spend")
    parser.add_argument(
        "--details", action="store_true",
        help="Provide DB and table information.")
    parser.add_argument(
        "--customer", action="store",
        help="Customer code to query. Format: CUST0123456789")
    parser.add_argument(
        "--product", action="store",
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
        "--count", action="store_true",
        help="Return total record counts.")
    parser.add_argument(
        "--spend", action="store_true",
        help="Return total spend for the query.")

    args = parser.parse_args()

    return parser, args


def output_type(record_type, result):

    """Handles the type of record we want outputted,
    for example for standard queries we might want raw records.
    A count instead, we might want an integer."""

    if record_type == "*":
        print(result)
    else:
        print(result[0][0])


#~ QUERIES FUNCTIONS start =======================
def all_records_from_product(
    product,
    record_type,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE PROD_CODE = '$product';""")

    try:
        cursor.execute(sql.substitute(
            record_type=record_type,
            table="dunn_humby",
            product=product))
        result = cursor.fetchall()
        output_type(record_type, result)
    except Exception as e:
        print(e)


def all_records_from_hour(
    hour,
    record_type,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE SHOP_HOUR = '$hour';""")

    try:
        cursor.execute(sql.substitute(
            record_type=record_type,
            table="dunn_humby",
            hour=hour))
        result = cursor.fetchall()
        output_type(record_type, result)
    except Exception as e:
        print(e)


def all_records_from_date(
    date,
    record_type,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE SHOP_DATE = '$date';""")

    try:
        cursor.execute(sql.substitute(
            record_type=record_type,
            table="dunn_humby",
            date=date))
        result = cursor.fetchall()
        output_type(record_type, result)
    except Exception as e:
        print(e)


def all_records_from_week(
    week,
    record_type,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE SHOP_WEEK = '$week';""")

    try:
        cursor.execute(sql.substitute(
            record_type=record_type,
            table="dunn_humby",
            week=week))
        result = cursor.fetchall()
        output_type(record_type, result)
    except Exception as e:
        print(e)


def all_records_from_weekday(
    weekday,
    record_type,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE SHOP_WEEKDAY = '$weekday';""")

    try:
        cursor.execute(sql.substitute(
            record_type=record_type,
            table="dunn_humby",
            weekday=weekday))
        result = cursor.fetchall()
        output_type(record_type, result)
    except Exception as e:
        print(e)

#~ CUSTOMER queries =========================
def customer_records_all(
    customer,
    record_type,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE CUST_CODE = '$customer';""")

    try:
        cursor.execute(sql.substitute(
            record_type=record_type,
            table="dunn_humby",
            customer=customer))
        result = cursor.fetchall()
        output_type(record_type, result)
    except Exception as e:
        print(e)


def customer_records_from_date(
    customer,
    date,
    record_type,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE SHOP_DATE = '$date'
        AND CUST_CODE = '$customer';""")

    try:
        cursor.execute(sql.substitute(
            record_type=record_type,
            table="dunn_humby",
            customer=customer,
            date=date))
        result = cursor.fetchall()
        output_type(record_type, result)
    except Exception as e:
        print(e)


def customer_records_from_week(
    customer,
    week,
    record_type,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE CUST_CODE = '$customer'
        AND SHOP_WEEK = '$week';""")

    try:
        cursor.execute(sql.substitute(
            record_type=record_type,
            table="dunn_humby",
            customer=customer,
            week=week))
        result = cursor.fetchall()
        output_type(record_type, result)
    except Exception as e:
        print(e)


def customer_records_from_weekday(
    customer,
    weekday,
    record_type,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE CUST_CODE = '$customer'
        AND SHOP_WEEKDAY = '$weekday';""")

    try:
        cursor.execute(sql.substitute(
            record_type=record_type,
            table="dunn_humby",
            customer=customer,
            weekday=weekday))
        result = cursor.fetchall()
        output_type(record_type, result)
    except Exception as e:
        print(e)


#~ CUSTOMER RECORDS TEMPORALLY WITH PRODUCT ======
def customer_records_for_product_from_date(
    customer,
    date,
    product,
    record_type,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE CUST_CODE = '$customer'
        AND SHOP_DATE = '$date'
        AND PROD_CODE = '$product';""")

    try:
        cursor.execute(sql.substitute(
            record_type=record_type,
            table="dunn_humby",
            customer=customer,
            date=date,
            product=product))
        result = cursor.fetchall()
        output_type(record_type, result)
    except Exception as e:
        print(e)


def customer_records_for_product_from_week(
    customer,
    week,
    product,
    record_type,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE CUST_CODE = '$customer'
        AND SHOP_WEEK = '$week'
        AND PROD_CODE = '$product';""")

    try:
        cursor.execute(sql.substitute(
            record_type=record_type,
            table="dunn_humby",
            customer=customer,
            week=week,
            product=product))
        result = cursor.fetchall()
        output_type(record_type, result)
    except Exception as e:
        print(e)


def customer_records_for_product_from_weekday(
    customer,
    weekday,
    product,
    record_type,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE CUST_CODE = '$customer'
        AND SHOP_WEEKDAY = '$weekday'
        AND PROD_CODE = '$product';""")

    try:
        cursor.execute(sql.substitute(
            record_type=record_type,
            table="dunn_humby",
            customer=customer,
            weekday=weekday,
            product=product))
        result = cursor.fetchall()
        output_type(record_type, result)
    except Exception as e:
        print(e)


#~ BASKET QUERIES ==========================
def basket_all_records(
    basket,
    record_type,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE BASKET_ID = '$basket';""")

    try:
        cursor.execute(sql.substitute(
            record_type=record_type,
            table="dunn_humby",
            basket=basket))
        result = cursor.fetchall()
        output_type(record_type, result)
    except Exception as e:
        print(e)


#~ GENERAL STATUS QUERY =====================
def db_details(
    cursor,
    connection):

    """
    Return some information about the current state of Postgres.
    """

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
        cursor.execute(sql_column_count.substitute(table="dunn_humby"))
        column_count = cursor.fetchall()
        cursor.execute(sql_record_count.substitute(table="dunn_humby"))
        record_count = cursor.fetchall()
        cursor.execute(sql_cust_count.substitute(table="dunn_humby"))
        cust_count = cursor.fetchall()
        cursor.execute(sql_basket_count.substitute(table="dunn_humby"))
        basket_count = cursor.fetchall()
        cursor.execute(sql_date_count.substitute(table="dunn_humby"))
        date_count = cursor.fetchall()
        print(f"DB connection details:\n", connection.get_dsn_parameters())
        print(f"\ndunn_humby details:\nRecords:     {record_count[0][0]}")
        print(f"Columns:     {column_count[0][0]}")
        print(f"Customers:   {cust_count[0][0]}")
        print(f"Baskets:     {basket_count[0][0]}")
        print(f"Shop dates:  {date_count[0][0]}")
    except Exception as e:
        print(e)


#~ main =================================
def main():

    try:

        parser, args = args_setup()
        if len(sys.argv) < 2:
            parser.print_help(sys.stderr)
            sys.exit(1)

         #~ Create connection using psycopg2
        try:
            connection = psycopg2.connect(**db_config.config)
        except psycopg2.OperationalError as e:
            if str(e).__contains__("does not exist"):
                print(f"\n!!! Default transactional epidemiology DB (TE_DB) not found.")
                print(f"!!! Please create the database before starting with this command:")
                print(f"\ncreatedb TE_DB")
            else:
                print("\n!!! There was a problem connecting to Postgres:\n{e}")
            sys.exit(1)

        #~ Create a cursor object
        cursor = connection.cursor()

        #~ check table exists
        cursor.execute(f"""
            SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='dunn_humby');""")
        if not cursor.fetchone()[0]:
            print(f"\n!!! Table 'dunn_humby' doesn't exist.")
            cursor.execute(f"""
                SELECT * FROM information_schema.tables
                WHERE table_schema = 'public';""")
            result = cursor.fetchall()
            table_list = ""
            for tab in result:
                table_list = table_list + tab[2] + ", "
            if len(table_list) == 0:
                print(f"The database currently contains no tables.")
            else:
                print(f"Tables currently in TE_DB: {table_list}")
            sys.exit(1)

        if args.details:
            db_details(
                cursor,
                connection)
            sys.exit(0)

        #~ if args.count is not included, SELECTs will be for all records,
        #~ flip this to COUNT or SPEND if args request
        record_type = "*"
        if args.count and args.spend:
            print(f"\n!!! Please provide just one record type (spend / count / etc).")
            sys.exit(1)
        if args.count:
            record_type = "COUNT(*)"
        if args.spend:
            record_type = "SUM(SPEND)"

    #~ three args ========================
        if args.customer and args.date and args.product:
            customer_records_for_product_from_date(
                args.customer,
                args.date,
                args.product,
                record_type,
                cursor,
                connection)
            connection.close()
            return

        if args.customer and args.week and args.product:
            customer_records_for_product_from_week(
                args.customer,
                args.week,
                args.product,
                record_type,

                cursor,
                connection)
            connection.close()
            return

        if args.customer and args.weekday and args.product:
            customer_records_for_product_from_weekday(
                args.customer,
                args.weekday,
                args.product,
                record_type,
                cursor,
                connection)
            connection.close()
            return

    #~ two args ==========================
        if args.customer and args.date:
            customer_records_from_date(
                args.customer,
                args.date,
                record_type,
                cursor,
                connection)
            connection.close()
            return

        if args.customer and args.week:
            customer_records_from_week(
                args.customer,
                args.week,
                record_type,
                cursor,
                connection)
            connection.close()
            return

        if args.customer and args.weekday:
            customer_records_from_weekday(
                args.customer,
                args.weekday,
                record_type,
                cursor,
                connection)
            connection.close()
            return

    #~ one arg ===========================
        if args.customer:
            customer_records_all(
                args.customer,
                record_type,
                cursor,
                connection)
            connection.close()
            return

        if args.product:
            all_records_from_product(
                args.product,
                record_type,
                cursor,
                connection)
            connection.close()
            return

        if args.hour:
            all_records_from_hour(
                args.hour,
                record_type,
                cursor,
                connection)
            connection.close()
            return

        if args.date:
            all_records_from_date(
                args.date,
                record_type,
                cursor,
                connection)
            connection.close()
            return

        if args.week:
            all_records_from_week(
                args.week,
                record_type,
                cursor,
                connection)
            connection.close()
            return

        if args.weekday:
            all_records_from_weekday(
                args.weekday,
                record_type,
                cursor,
                connection)
            connection.close()
            return

        if args.basket:
            basket_all_records(
                args.basket,
                record_type,
                cursor,
                connection)
            connection.close()
            return

    except KeyboardInterrupt:
        print("OK, stopping.")
        try:
            connection.close()
        except Exception as e:
            print(e)

    except Exception:
        traceback.print_exc(file=sys.stdout)

    sys.exit(0)


if __name__ == "__main__":

    main()

