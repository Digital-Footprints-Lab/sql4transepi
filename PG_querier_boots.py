
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

#~ QUERIES FUNCTIONS start #########################
#~ FOUR query args #################################
def customer_records_for_product_from_date_from_store():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 = '$date'
            AND ITEM_CODE = '$product'
            AND STORE LIKE '%$store%'
            AND ID = '$customer';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 = '$date'
            AND ITEM_CODE = '$product'
            AND STORE LIKE '%$store%'
            AND ID = '$customer';""")


def customer_records_for_product_from_date_range_from_store():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND ITEM_CODE = '$product'
            AND STORE LIKE '%$store%'
            AND ID = '$customer';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND ITEM_CODE = '$product'
            AND STORE LIKE '%$store%'
            AND ID = '$customer';""")


#~ THREE query args ################################
def customer_records_from_store_from_date():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 = '$date'
            AND STORE LIKE '%$store%'
            AND ID = '$customer';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 = '$date'
            AND STORE LIKE '%$store%'
            AND ID = '$customer';""")


def customer_records_from_store_from_date_range():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND STORE LIKE '%$store%'
            AND ID = '$customer';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND STORE LIKE '%$store%'
            AND ID = '$customer';""")


def product_records_from_store_from_date():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 = '$date'
            AND ITEM_CODE = '$product'
            AND STORE LIKE '%$store%';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 = '$date'
            AND ITEM_CODE = '$product'
            AND STORE LIKE '%$store%';""")


def product_records_from_store_from_date_range():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND ITEM_CODE = '$product'
            AND STORE LIKE '%$store%';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND ITEM_CODE = '$product'
            AND STORE LIKE '%$store%';""")


def customer_records_for_product_from_store():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE ID = '$customer'
            AND ITEM_CODE = '$product'
            AND STORE LIKE '%$store%';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE ID = '$customer'
            AND ITEM_CODE = '$product'
            AND STORE LIKE '%$store%';""")


def customer_records_for_product_from_date():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE ID = '$customer'
            AND DATE2 = '$date'
            AND ITEM_CODE = '$product';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE ID = '$customer'
            AND DATE2 = '$date'
            AND ITEM_CODE = '$product';""")


def customer_records_for_product_from_date_range():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE ID = '$customer'
            AND DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND ITEM_CODE = '$product';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE ID = '$customer'
            AND DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND ITEM_CODE = '$product';""")


#~ TWO query args ##################################
def customer_records_for_product():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE ID = '$customer'
            AND ITEM_CODE = '$product';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE ID = '$customer'
            AND ITEM_CODE = '$product';""")


def customer_records_from_date():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 = '$date'
            AND ID = '$customer';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 = '$date'
            AND ID = '$customer';""")


def customer_records_from_date_range():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND ID = '$customer';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND ID = '$customer';""")


def product_records_for_date():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 = '$date'
            AND ITEM_CODE = '$product';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 = '$date'
            AND ITEM_CODE = '$product';""")


def product_records_for_date_range():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND ITEM_CODE = '$product';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND ITEM_CODE = '$product';""")


def store_records_for_customer():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE STORE LIKE '%$store%'
            AND ID = '$customer';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE STORE LIKE '%$store%'
            AND ID = '$customer';""")


def store_records_for_product():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE STORE LIKE '%$store%'
            AND ITEM_CODE = '$product';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE STORE LIKE '%$store%'
            AND ITEM_CODE = '$product';""")


def store_records_for_date():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE STORE LIKE '%$store%'
            AND DATE2 = '$date';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE STORE LIKE '%$store%'
            AND DATE2 = '$date';""")


def store_records_for_date_range():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE STORE LIKE '%$store%'
            AND DATE2 >= '$start_date'
            AND DATE2 <= '$end_date';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE STORE LIKE '%$store%'
            AND DATE2 >= '$start_date'
            AND DATE2 <= '$end_date';""")


#~ ONE query arg ###################################
def all_records_from_product():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE ITEM_CODE = '$product';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE ITEM_CODE = '$product';""")


def all_records_from_date():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 = '$date';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 = '$date';""")


def all_records_from_date_range():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date';""")


def all_records_from_customer():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE ID = '$customer';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE ID = '$customer';""")


def all_records_from_store():

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE STORE LIKE '%$store%';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE STORE LIKE '%$store%';""")


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


#~ main ############################################
def main():

    try:

        #~ Create connection using psycopg2
        connection, cursor = PG_status.connect_to_postgres(db_config)

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
        if args.store:
            store_w_brackets = "(" + args.store + ")"

        #~ long-winded I know, but the following are the 15 combinations of
        #~ the four possible args given (customer, store, date, product):
        #~ four args #####################################
        if args.customer and args.date and args.product and args.store:
            if len(args.date) == 1:
                sql = customer_records_for_product_from_date_from_store()
            if len(args.date) == 2:
                sql = customer_records_for_product_from_date_range_from_store()

        #~ three args ####################################
        if args.customer and args.date and args.product:
            if len(args.date) == 1:
                sql = customer_records_for_product_from_date()
            if len(args.date) == 2:
                sql = customer_records_for_product_from_date_range()

        if args.customer and args.date and args.store:
            if len(args.date) == 1:
                sql = customer_records_from_store_from_date()
            if len(args.date) == 2:
                sql = customer_records_from_store_from_date_range()

        if args.date and args.product and args.store:
            if len(args.date) == 1:
                sql = product_records_from_store_from_date()
            if len(args.date) == 2:
                sql = product_records_from_store_from_date_range()

        if args.customer and args.product and args.store:
            sql = customer_records_for_product_from_store()

        #~ two args ######################################
        if args.customer and args.date:
            if len(args.date) == 1:
                sql = customer_records_from_date()
            if len(args.date) == 2:
                sql = customer_records_from_date_range()

        if args.customer and args.product:
            sql = customer_records_for_product()

        if args.store and args.customer:
            sql = store_records_for_customer()

        if args.store and args.product:
            sql = store_records_for_product()

        if args.store and args.date:
            if len(args.date) == 1:
                sql = store_records_for_date()
            if len(args.date) == 2:
                sql = store_records_for_date_range()

        if args.product and args.date:
            if len(args.date) == 1:
                sql = product_records_for_date()
            if len(args.date) == 2:
                sql = product_records_for_date_range()

        #~ one arg #######################################
        if args.customer:
            sql = all_records_from_customer()

        if args.product:
            sql = all_records_from_product()

        if args.store:
            sql = all_records_from_store()

        if args.date:
            print("bob")
            if len(args.date) == 1:
                sql = all_records_from_date()
            if len(args.date) == 2:
                sql = all_records_from_date_range()

        try:
            cursor.execute(sql.substitute(
                customer = args.customer,
                product = args.product,
                date = args.date[0],
                start_date = start_date,
                end_date = end_date,
                store = args.store,
                record_type = record_type,
                card_table = db_config.boots_transactions,
                product_table = db_config.boots_products,
                table = db_config.boots_transactions,))
            result = cursor.fetchall()
            connection.close()
            return output_type(record_type, result, write_csv,)
        except Exception as e:
            print(e)

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
    #~ trigger for LEFT JOIN between transactions and products.
    #~ we use LEFT JOIN because we DO want
    #~ results without matches to return with empty fields.
    join = False
    if args.join:
        join = True
    #~ trigger to write to csv file
    write_csv = False
    if args.write_csv:
        write_csv = True
    #~ date range trigger
    start_date = None
    end_date = None
    if args.date and len(args.date) == 2:
            start_date = args.date[0]
            end_date = args.date[1]

    ####~ main runner
    records = main()
    ################~

    if args.write_csv:
        write_to_csv(args.write_csv, records, write_csv,)

