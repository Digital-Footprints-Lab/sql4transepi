
#~ Standard library imports
import sys
import os
import traceback
import re
from string import Template
import argparse
import pprint
import csv

#~ 3rd party imports
import psycopg2

#~ local imports
import db_config
import PG_status


def args_setup():

    parser = argparse.ArgumentParser(
        description = "PostgreSQL DB Querier: Boots transaction data",
        epilog = "Example: python boots_PG_querier.py --customer 9874786793 --date 20180621 --spend")
    parser.add_argument(
        "--details", action = "store_true",
        help = "Provide DB and table information.")
    parser.add_argument(
        "--product_table", action = "store",
        help = "The name of the product table to query.")
    parser.add_argument(
        "--customer", action = "store",
        help = "Customer code to query. Format: 9874786793")
    parser.add_argument(
        "--product", action = "store",
        help = "Product code to query. Format: 8199922")
    parser.add_argument(
        "--date", nargs = "+", action = "store",
        help = "Shop date (or date range) to query. Format: YYYYMMDD (provide two dates for a range)")
    parser.add_argument(
        "--count", action = "store_true",
        help = "Return total record counts.")
    parser.add_argument(
        "--spend", action = "store_true",
        help = "Return total spend for the query.")
    parser.add_argument(
        "--join", action = "store_true",
        help = "Return card transaction items JOINed with product information.")

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


def write_to_csv(filename, records, join=None):

    fields = [
        "ID",
        "DATE2",
        "TIME3",
        "STORE",
        "PAYMENT",
        "STAFF_DISCOUNT_CARD_NUMBER",
        "ITEM_CODE",
        "ITEM_DESCRIPTION","POINTS_ADJUSTMENT",
        "POINTS_ITEM","UNITS",
        "SPEND",
        "DISCOUNT",]

    join_fields = [
        "index",
        "product_link",
        "productid",
        "name",
        "PDP_productPrice",
        "details",
        "long_description",]

    if join:
        fields.extend(join_fields)

    with open(filename, "w") as file:

        write = csv.writer(file)
        write.writerow(fields)
        write.writerows(records)


#~ QUERIES FUNCTIONS start #################################
def all_records_from_product(
    product,
    record_type,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE ITEM_CODE = '$product';""")

    try:
        cursor.execute(sql.substitute(
            record_type = record_type,
            table = "boots_transactions",
            product = product))
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
        WHERE DATE2 = '$date';""")

    try:
        cursor.execute(sql.substitute(
            record_type = record_type,
            table = "boots_transactions",
            date = date))
        result = cursor.fetchall()
        output_type(record_type, result)
    except Exception as e:
        print(e)


def all_records_from_date_range(
    start_date,
    end_date,
    record_type,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE DATE2 >= '$start_date'
        AND DATE2 <= '$end_date';""")

    try:
        cursor.execute(sql.substitute(
            record_type = record_type,
            table = "boots_transactions",
            start_date = start_date,
            end_date = end_date))
        result = cursor.fetchall()
        output_type(record_type, result)
    except Exception as e:
        print(e)


#~ CUSTOMER queries #################################
def customer_records_all(
    customer,
    record_type,
    cursor,
    connection,
    join=False):

    try:
        if join:
            sql = Template("""
                SELECT $record_type FROM $table
                INNER JOIN $product_table
                ON $card_table.ITEM_CODE = $product_table.productid
                WHERE ID = '$customer';""")
            cursor.execute(sql.substitute(
                record_type = record_type,
                card_table = "boots_transactions",
                product_table = "boots_products",
                table = "boots_transactions",
                customer = customer))
            result = cursor.fetchall()
            output_type(record_type, result)
        else:
            sql = Template("""
                SELECT $record_type FROM $table
                WHERE ID = '$customer';""")
            cursor.execute(sql.substitute(
                record_type = record_type,
                table = "boots_transactions",
                customer = customer))
            result = cursor.fetchall()
            output_type(record_type, result)
    except Exception as e:
        print(e)


def customer_records_for_product(
    customer,
    product,
    record_type,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE ID = '$customer'
        AND ITEM_CODE = '$product';""")

    try:
        cursor.execute(sql.substitute(
            record_type = record_type,
            table = "boots_transactions",
            customer = customer,
            product = product))
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
        WHERE DATE2 = '$date'
        AND ID = '$customer';""")

    try:
        cursor.execute(sql.substitute(
            record_type = record_type,
            table = "boots_transactions",
            customer = customer,
            date = date))
        result = cursor.fetchall()
        output_type(record_type, result)
    except Exception as e:
        print(e)


def customer_records_from_date_range(
    customer,
    start_date,
    end_date,
    record_type,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE DATE2 >= '$start_date'
        AND DATE2 <= '$end_date'
        AND ID = '$customer';""")

    try:
        cursor.execute(sql.substitute(
            record_type = record_type,
            table = "boots_transactions",
            customer = customer,
            start_date = start_date,
            end_date = end_date))
        result = cursor.fetchall()
        output_type(record_type, result)
    except Exception as e:
        print(e)


#~ CUSTOMER RECORDS TEMPORALLY WITH PRODUCT ################
def customer_records_for_product_from_date(
    customer,
    date,
    product,
    record_type,
    cursor,
    connection):

    sql = Template("""
        SELECT $record_type FROM $table
        WHERE ID = '$customer'
        AND DATE2 = '$date'
        AND ITEM_CODE = '$product';""")

    try:
        cursor.execute(sql.substitute(
            record_type = record_type,
            table = "boots_transactions",
            customer = customer,
            date = date,
            product = product))
        result = cursor.fetchall()
        output_type(record_type, result)
    except Exception as e:
        print(e)

#~ inter-table operations #######################
def join_on_product_id(
    record_type,
    product_table,
    cursor,
    connection):

    """
    The card transaction table contains each item purchased, but without
    further product information. The product table contain information,
    and can be linked to the transaction table. So, this function takes the
    product ID numbers common to both tables and does a JOIN on them.
    """

    sql = Template("""
        SELECT $record_type FROM $card_table
        INNER JOIN $product_table
        ON $card_table.ITEM_CODE = $product_table.productid;""")

    try:
        cursor.execute(sql.substitute(
            record_type = record_type,
            card_table = "boots_transactions",
            product_table = "boots_products"))
        result = cursor.fetchall()
        output_type(record_type, result)
    except Exception as e:
        print(e)


#~ GENERAL STATUS QUERY ##############################
def db_details(
    cursor,
    connection):

    """
    Return some information about the current state of Postgres.
    """

    sql_card_column_count = Template("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name = '$table';""")
    sql_card_record_count = Template("""
        SELECT COUNT(*)
        FROM $table;""")
    sql_product_column_count = Template("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name = '$table';""")
    sql_product_record_count = Template("""
        SELECT COUNT(*)
        FROM $table;""")

    try:
        cursor.execute(sql_card_column_count.substitute(table = "boots_transactions"))
        card_column_count = cursor.fetchall()
        cursor.execute(sql_card_record_count.substitute(table = "boots_transactions"))
        card_record_count = cursor.fetchall()
        cursor.execute(sql_product_column_count.substitute(table = "boots_products"))
        product_column_count = cursor.fetchall()
        cursor.execute(sql_product_record_count.substitute(table = "boots_products"))
        product_record_count = cursor.fetchall()
        print(f"\nboots_transactions details:\n")
        pprint.pprint(connection.get_dsn_parameters())
        print(f"\nTable name:    boots_transactions\nColumns:       {card_column_count[0][0]}")
        print(f"Records:       {card_record_count[0][0]}")
        print(f"\nTable name:    boots_products\nColumns:       {product_column_count[0][0]}")
        print(f"Records:       {product_record_count[0][0]}")
        print(f"\nAbove are some details about the current DB. Please provide a query.")
        print(f"For help: python boots_PG_querier.py --help")
    except Exception as e:
        print(e)


#~ main ############################################
def main():

    try:

        parser, args = args_setup()
        if len(sys.argv) < 2:
            parser.print_help(sys.stderr)
            sys.exit(1)

        #~ Create connection using psycopg2
        connection, cursor = PG_status.connect_to_postgres(db_config)

        #~ Return some DB details if no query args are given
        if args.details or not any([
            args.product,
            args.product_table,
            args.customer,
            args.date,
            args.count,
            args.spend,
            args.join]):
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

        join = False
        if args.join:
            join = True

    #~ three args ##########################################
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

    #~ two args #########################################
        if args.customer and args.date:
            if len(args.date) == 1:
                customer_records_from_date(
                    args.customer,
                    args.date[0],
                    record_type,
                    cursor,
                    connection)
                connection.close()
                return
            if len(args.date) == 2:
                customer_records_from_date_range(
                    args.customer,
                    args.date[0],
                    args.date[1],
                    record_type,
                    cursor,
                    connection)
                connection.close()
                return

        if args.customer and args.product:
            customer_records_for_product(
                args.customer,
                args.product,
                record_type,
                cursor,
                connection)
            connection.close()
            return

    #~ one arg #############################################
        if args.customer:
            customer_records_all(
                args.customer,
                record_type,
                cursor,
                connection,
                join)
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

        if args.date:
            if len(args.date) == 1:
                all_records_from_date(
                    args.date[0],
                    record_type,
                    cursor,
                    connection)
                connection.close()
                return
            if len(args.date) == 2:
                all_records_from_date_range(
                    args.date[0],
                    args.date[1],
                    record_type,
                    cursor,
                    connection)
                connection.close()
                return

        if args.join:
            join_on_product_id(
                record_type,
                args.product_table,
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
        traceback.print_exc(file = sys.stdout)

    sys.exit(0)


if __name__ == "__main__":

    main()

