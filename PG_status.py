
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


def args_setup():

    parser = argparse.ArgumentParser(
        description = "PostgreSQL DB Status Reporter",
        epilog = "Example: python PG_status.py --tables")
    parser.add_argument(
        "--tables", action = "store_true",
        help = "Provide table information.")
    parser.add_argument(
        "--connection", action = "store_true",
        help = "Provide DB connection information.")
    parser.add_argument(
        "--drop_table", action = "store",
        help = "Delete table from DB. Be careful, this operation is permanent.")

    args = parser.parse_args()

    return parser, args


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
            print("\n!!! Problem connecting to Postgres:\n{e}")
        sys.exit(1)

    return connection, cursor


def db_details(host, user):

    """
    Checks details of DBs present in Postgres.
    Returns:
        list:   [dbs]
        str:    dbs as string
    """

    #~ Create connection using psycopg2
    connection, cursor = connect_to_postgres(db_config)

    #~ get the name of the DBs present, filtering out defaults
    try:
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
    except Exception as e:
        print("!!! Problem getting DB details:", e)
        return 1

    if len(db_names) == 1:
        print(f"Postgres is managing 1 DB: \n{db_pretty}")
    else:
        print(f"Postgres is managing {len(db_names)} DBs: \n{db_pretty}")


def table_details(cursor):

    """
    Checks details of tables present in Postgres.
    Returns:
        list:   [dbs]
        str:    dbs as string
    """

    #~ Create connection using psycopg2
    connection, cursor = connect_to_postgres(db_config)

    try:
        cursor.execute(f"""
            SELECT * FROM information_schema.tables
            WHERE table_schema = 'public';""")
        result = cursor.fetchall()
    except Exception as e:
        print("!!! Problem getting table details:", e)
        return 1

    table_list = []
    for tab in result:
        table_list.append(tab[2])
    if len(table_list) == 0:
        print(f"\nThe database currently contains no tables.")
    else:
        print(f"\nte_db contains the following tables:")
        print(*table_list, sep="\n")

    return table_list


def drop_table(table):

    #~ Create connection using psycopg2
    connection, cursor = connect_to_postgres(db_config)

    sql_record_count = Template("""
        SELECT COUNT(*)
        FROM $table;""")

    try:
        cursor.execute(sql_record_count.substitute(table=table))
    except Exception as e:
        print(f"\n!!! There is no table called '{table}' in the DB.")
        table_details(cursor)
        return 1

    sql = Template(f"""DROP TABLE IF EXISTS $table;""")

    try:
        cursor.execute(sql.substitute(table=table))
        connection.commit()
        print(f"OK, table '{table}' dropped.")
    except Exception as e:
        print("!!! Problem dropping table:", e)


def main():

    parser, args = args_setup()

    #~ Create connection using psycopg2
    connection, cursor = connect_to_postgres(db_config)

    if len(sys.argv) < 2:
        parser.print_help(sys.stderr)
        print("\n" + 42 * "=")
        db_details(
            db_config.config["host"],
            db_config.config["user"],)
        table_details(cursor)
        print(42 * "=")
        sys.exit(0)

    #~ Query the status of the default tables
    if args.tables:
        #~ we have to refresh the connection after each check,
        #~ thus the loop doing a connect each time.
        for func in [CSV2PG_tesco_card, CSV2PG_boots_card,
                     CSV2PG_boots_scrape, CSV2PG_foodproducts]:
            func.table_details(connection, cursor)
            connection, cursor = connect_to_postgres(db_config)

    if args.connection:
        connection_details = connection.get_dsn_parameters()
        print(f"\nDB connection details:\n")
        for thing in connection_details:
            print (thing, ":", connection_details[thing])

    if args.drop_table:
        drop_table(args.drop_table)


if __name__ == "__main__":

    main()
