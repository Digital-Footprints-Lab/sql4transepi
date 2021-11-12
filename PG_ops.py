
#~ Standard library imports
import sys
import os
import subprocess
import re
import argparse
from string import Template
import csv
import codecs
import shutil

#~ 3rd party imports
import chardet
import psycopg2
from psycopg2 import Error

#~ local imports
# import db_config
from CSV2PG_tesco_card import table_details as tesco_details
from CSV2PG_boots_card import table_details as boots_details
# from CSV2PG_boots_scrape import table_details as boots_products_details
# from CSV2PG_tesco_card import table_details as tesco_details


def connect_to_postgres(db_config):

    try:
        connection = psycopg2.connect(**db_config.config)
        cursor = connection.cursor()
    except psycopg2.OperationalError as e:
        if str(e).__contains__("does not exist"):
            print(f"\n!!! Default transactional epidemiology DB (te_db) not found.")
            print(f"!!! Please create the database before starting with this command:")
            print(f"\ncreatedb te_db")
        else:
            print("\n!!! There was a problem connecting to Postgres:\n{e}")
        sys.exit(1)

    return connection, cursor


def drop_table(table):
    pass


def main():

    tesco_details(connection, cursor)
    boots_details()


if __name__ == "__main__":

    main()