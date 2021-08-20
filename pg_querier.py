import sys
import os
import re
import argparse
import csv
import pandas as pd
import psycopg2


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
        "--cust", "--customer", nargs="+", action="store",
        help="Customer code to query. Format: CUST0123456789")

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

def all_customer_records(
    db,
    table,
    cursor,
    connection,
    customer):





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




if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        print("OK, stopping.")
