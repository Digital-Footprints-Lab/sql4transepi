
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
import queries


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
        "--store", action = "store",
        help = "The store code for the shop you want to query. I think these are usually four digits, eg: 6565")
    parser.add_argument(
        "--count", action = "store_true",
        help = "Return total record counts.")
    parser.add_argument(
        "--spend", action = "store_true",
        help = "Return total spend for the query.")
    parser.add_argument(
        "--join", action = "store_true",
        help = "Return card transaction items from your query, JOINed with product information.")
    parser.add_argument(
        "--write_csv", action = "store",
        help = "Write the results to a CSV file (specify the filename).")

    args = parser.parse_args()

    return parser, args


def output_type(record_type, result, write_csv,):

    """Handles the type of record we want outputted,
    for example for standard queries we might want raw records.
    A count instead, we might want an integer."""

    if record_type == "*":
        if not write_csv:
            print(result)
        return result
    else:
        if not write_csv:
            print(result[0][0])
        return result[0][0]


def write_to_csv(filename, records, join=False,):

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

    print(f"OK, written {len(records)} records to {file.name}.")


#~ GENERAL STATUS QUERY ##############################
def db_details(
    cursor,
    connection,
    join=False,):

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
        cursor.execute(sql_card_column_count.substitute(table = db_config.boots_transactions))
        card_column_count = cursor.fetchall()
        cursor.execute(sql_card_record_count.substitute(table = db_config.boots_transactions))
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


def arg_triggers():

    #~ date range trigger
    date = None
    start_date = None
    end_date = None
    if args.date and len(args.date) == 2:
        start_date = args.date[0]
        end_date = args.date[1]
    elif args.date:
        date = args.date[0]

    #~ trigger to write to csv file
    write_csv = False
    if args.write_csv:
        write_csv = True

    #~ trigger for LEFT JOIN between transactions and products.
    #~ we use LEFT JOIN because we DO want
    #~ results without matches to return with empty fields.
    join = False
    if args.join:
        join = True

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

    #~ store numbers are in a VARCHAR field with brackets around,
    #~ so we want to add those brackets to avoid erroneous
    #~ substring matching, say, 786 with 1786
    store_w_brackets = None
    if args.store:
        store_w_brackets = "(" + args.store + ")"

    return date, start_date, end_date, join, write_csv, record_type, store_w_brackets


#~ main ############################################
def main():

    try:

        #~ Return some DB details if no query args are given
        if args.details or not any([
            args.product,
            args.product_table,
            args.customer,
            args.date,
            args.store,
            args.count,
            args.spend,
            args.join]):

            db_details(
                cursor,
                connection,)
            sys.exit(0)

        #~ long-winded I know, but the following are the 15 combinations of
        #~ the four possible args given (customer, store, date, product):
        #~ four args #####################################
        if args.customer and args.date and args.product and args.store:
            if len(args.date) == 1:
                sql = queries.customer_records_for_product_from_date_from_store(join)
            if len(args.date) == 2:
                sql = queries.customer_records_for_product_from_date_range_from_store(join)

        #~ three args ####################################
        elif args.customer and args.date and args.product:
            if len(args.date) == 1:
                sql = queries.customer_records_for_product_from_date(join)
            if len(args.date) == 2:
                sql = queries.customer_records_for_product_from_date_range(join)

        elif args.customer and args.date and args.store:
            if len(args.date) == 1:
                sql = queries.customer_records_from_store_from_date(join)
            if len(args.date) == 2:
                sql = queries.customer_records_from_store_from_date_range(join)

        elif args.date and args.product and args.store:
            if len(args.date) == 1:
                sql = queries.product_records_from_store_from_date(join)
            if len(args.date) == 2:
                sql = queries.product_records_from_store_from_date_range(join)

        elif args.customer and args.product and args.store:
            sql = queries.customer_records_for_product_from_store(join)

        #~ two args ######################################
        elif args.customer and args.date:
            if len(args.date) == 1:
                sql = queries.customer_records_from_date(join)
            if len(args.date) == 2:
                sql = queries.customer_records_from_date_range(join)

        elif args.customer and args.product:
            sql = queries.customer_records_for_product(join)

        elif args.store and args.customer:
            sql = queries.store_records_for_customer(join)

        elif args.store and args.product:
            sql = queries.store_records_for_product(join)

        elif args.store and args.date:
            if len(args.date) == 1:
                sql = queries.store_records_for_date(join)
            if len(args.date) == 2:
                sql = queries.store_records_for_date_range(join)

        elif args.product and args.date:
            if len(args.date) == 1:
                sql = queries.product_records_for_date(join)
            if len(args.date) == 2:
                sql = queries.product_records_for_date_range(join)

        #~ one arg #######################################
        elif args.customer:
            sql = queries.all_records_from_customer(join)

        elif args.product:
            sql = queries.all_records_from_product(join)

        elif args.store:
            sql = queries.all_records_from_store(join)

        elif args.date:
            if len(args.date) == 1:
                sql = queries.all_records_from_date(join)
            if len(args.date) == 2:
                sql = queries.all_records_from_date_range(join)

        #~ Create connection using psycopg2
        connection, cursor = PG_status.connect_to_postgres(db_config)
        try:
            #~ modify the sql template with our args
            cursor.execute(sql.substitute(
                customer = args.customer,
                product = args.product,
                date = date,
                start_date = start_date,
                end_date = end_date,
                store = args.store,
                record_type = record_type,
                card_table = db_config.boots_transactions,
                product_table = db_config.boots_products,
                table = db_config.boots_transactions,))
            #~ submit the query
            result = cursor.fetchall()
            connection.close()
            return output_type(record_type, result, write_csv,)
        except Exception as e:
            print(f"There was a problem sumbitting the query:", e)

    except KeyboardInterrupt:
        print("OK, stopping.")
        try:
            connection.close()
        except Exception as e:
            print(e)

    except Exception:
        traceback.print_exc(file = sys.stdout)


if __name__ == "__main__":

    parser, args = args_setup()
    if len(sys.argv) < 2:
        parser.print_help(sys.stderr)
        sys.exit(1)

    #~ we need to fiddle a little with the args
    (date, start_date, end_date, join,
     write_csv, record_type, store_w_brackets,) = arg_triggers()

    ####~ main runner
    records = main()
    ################~

    if args.write_csv:
        write_to_csv(args.write_csv, records, write_csv,)

