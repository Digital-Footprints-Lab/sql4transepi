
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
import db_config
import CSV2PG_tesco_card
import CSV2PG_boots_card
import CSV2PG_boots_scrape
import CSV2PG_foodproducts


def connect_to_postgres(db_config):

    """
    Uses psycopg2 to create a connection with
    the local Postgres DB. db_config is a local
    python file with a dict of user, db name, IP etc

    Returns:
        connection object
        cursor object
    """

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


def db_details(host, user):

    """
    Checks what DBs are actually in operation here.
    Returns:
        list:   [dbs]
        str:    dbs as string
    """

    #~ apologies for subprocess :|
    records, _ = subprocess.Popen([
        'psql','-lA','-F\x02','-R\x01','-h',
        host,'-U',user ],
        stdout=subprocess.PIPE).communicate()
    #~ regex out the DB names.
    db_names = re.findall(r'x01(.*?)\\x02', str(records))
    #~ remove the default DBs
    default_db_names = [user, "postgres", "template0", "template1", "Name"]
    for _db in default_db_names:
        db_names.remove(_db)
    db_pretty = ", ".join(db_names)

    return db_names, db_pretty


def drop_table(table):
    pass


def main():

    #~ Create connection using psycopg2
    connection, cursor = connect_to_postgres(db_config)
    try:
        db_names, db_pretty = db_details(
            db_config.config["host"],
            db_config.config["user"],)
        print(f"\nPostgres currently contains {len(db_names)} DBs: \n{db_pretty}\n")
    except Exception as e:
        print(e)

    #! why isn't this working? is fine if they are all fine,
    #! but says they all fail if one fails <----
    #~ Query the status of the default tables
    try:
        CSV2PG_tesco_card.table_details(connection, cursor)
    except Exception as e:
        print("No Tescos Loyalty Card table present.")
    try:
        CSV2PG_boots_card.table_details(connection, cursor)
    except Exception as e:
        print("No Boots Loyalty Card table present.")
    try:
        CSV2PG_boots_scrape.table_details(connection, cursor)
    except Exception as e:
        print("No Boots products table present.")
    try:
        CSV2PG_foodproducts.table_details(connection, cursor)
    except Exception as e:
        print("No foodproducts table present.")


if __name__ == "__main__":

    main()