
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
        description = "PostgreSQL: Tesco Product Table Builder",
        epilog = "Example: python PG_tesco_table_builder.py")

    args = parser.parse_args()

    return parser, args


def create_product_table(connection, cursor):

    """
    Create a table for incoming products. These are being read
    from the tesco transaction table, being assigned an integer
    as a product ID. There is only one useful field in the transaction
    data: the product name. Other fields are either not used or inaccurate.
    """

    sql = Template("""
        CREATE TABLE IF NOT EXISTS $table (
        product_id INT,
        product_name UNIQUE VARCHAR);""")

    try:
        cursor.execute(sql.substitute(table="tesco_products"))
        connection.commit()
    except Exception as e:
        print(e)


def build_product_table(connection, cursor):

    """
    options:
    1. copy the whole thing, chop off columns and dups.
    2. go through each line, is it in other table, if not, in it goes
    3. something like
        CREATE TABLE tesco_products AS
        SELECT * FROM tesco_transactions;
        but dealing with dups and adding INT column?
    """

    #~ move each transaction item to product table on unique NAME
    sql = Template("""
        INSERT INTO $table
        SELECT * FROM tesco_transactions
        ON CONFLICT DO NOTHING;""")

    try:
        cursor.execute(sql.substitute(table="tesco_products"))
        connection.commit()
        print(f"\nOK, {csv.name} imported.")
    except Exception as e:
        print(e)


def main():

    #~ Create connection using psycopg2
    connection, cursor = PG_status.connect_to_postgres(db_config)


if __name__ == "__main__":

    main()

