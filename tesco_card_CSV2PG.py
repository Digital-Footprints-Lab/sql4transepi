
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


def args_setup():

    parser = argparse.ArgumentParser(
        description="Postgres DB Importer: Tesco Clubcard Loyalty Cards.",
        epilog="Example: python tesco_card_JSON2PG.py -d database1 -t table1 -i tescos_card1.csv")
    # parser.add_argument(
    #     "-d", "--db", action="store", required=True,
    #     help="The name of the DB to import to. This needs to have previously created.")
    parser.add_argument(
        "-t", "--table", action="store", required=True,
        help="The name of the table to import to, or to create if it doesn't exist.")
    parser.add_argument(
        "-i", "--input", type=argparse.FileType("r"), default=sys.stdin,
        metavar="PATH", required=True,
        help="JSON file to import.")

    args = parser.parse_args()

    return parser, args


def create_table(table, connection, cursor):

    """
    Create a table consistent with JSON product details, as
    provided on a loyalty card information request, and converted to CSV
    by the script tesco_card_JSON2CSV.py
    ie, there are three extra fields added to each product block
    1. customerid   2. basketid   3. storeid   4. timestamp
    (customerid is hash generated since there is no ID in tesco jsons)
    See tesco_card_JSON2CSV.py for more details
    """

    sql = Template("""
        CREATE TABLE IF NOT EXISTS $table (
        customer_id VARCHAR,
        basket_id VARCHAR,
        time_stamp TIMESTAMP,
        store_id INT,
        product_name VARCHAR,
        quantity INT,
        channel VARCHAR,
        weight_in_grams VARCHAR,
        item_selling_price FLOAT,
        volume_in_litres VARCHAR
        );""")

    try:
        cursor.execute(sql.substitute(table=table))
        connection.commit()
    except Exception as e:
        print(e)


def import_csv_to_pg_table(
    csv,
    table,
    connection,
    cursor):

    """
    Imports a CSV with columns consistent with Tesco loyalty card JSON fields.
    This is generated using the script tesco_card_JSON2CSV.py
    See function "create_table" for the fields we are extracting and making into columns.
    """

    print(f"\nImporting Tescos Card {csv.name} to Postgres table '{table}', just a moment...")

    dirname = os.path.dirname(__file__)
    csv_path = os.path.join(dirname, csv.name)

    sql = Template("""
        COPY $table (
        customer_id,
        basket_id,
        time_stamp,
        store_id,
        product_name,
        quantity,
        channel,
        weight_in_grams,
        item_selling_price,
        volume_in_litres)
        FROM '$csv_path' CSV HEADER;""")

    try:
        cursor.execute(sql.substitute(table=table, csv_path=csv_path))
        connection.commit()
        print(f"\nOK, {csv.name} imported.")
    except Exception as e:
        print(f"\n!!! Import failed: {csv.name} is not consistent with table fields.")
        print(f"!!! The csv format might not be correct, or you might be importing to the wrong table?")
        sys.exit(1)

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


def table_details(
    table,
    cursor,
    connection):

    """
    Return some information about the Tesco card import to Postgres.
    """

    sql_record_count = Template("""
        SELECT COUNT(*)
        FROM $table;""")
    sql_column_count = Template("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name='$table';""")
    sql_id_count = Template("""
        SELECT COUNT (DISTINCT customer_id) FROM $table;""")
    sql_item_count = Template("""
        SELECT COUNT (DISTINCT product_name) FROM $table;""")
    sql_date_count = Template("""
        SELECT COUNT (DISTINCT time_stamp) FROM $table;""")

    try:
        cursor.execute(sql_record_count.substitute(table=table))
        record_count = cursor.fetchall()
        cursor.execute(sql_column_count.substitute(table=table))
        column_count = cursor.fetchall()
        cursor.execute(sql_id_count.substitute(table=table))
        id_count = cursor.fetchall()
        cursor.execute(sql_item_count.substitute(table=table))
        item_count = cursor.fetchall()
        cursor.execute(sql_date_count.substitute(table=table))
        date_count = cursor.fetchall()
        print(f"\n{table} details:\nRecords:       {record_count[0][0]}")
        print(f"Column count:  {column_count[0][0]}")
        print(f"Customer IDs:  {id_count[0][0]}")
        print(f"Items:         {item_count[0][0]}")
        print(f"Shop dates:    {date_count[0][0]}")
    except Exception as e:
        print(e)


def main():

    parser, args = args_setup()
    if len(sys.argv) < 2:
        parser.print_help(sys.stderr)
        sys.exit(1)

    #~ Create connection using psycopg2
    try:
        connection = psycopg2.connect(**db_config.config)
    except psycopg2.OperationalError as e:
        if str(e).__contains__("does not exist"):
            print(f"\n!!! Default transactional epidemiology DB (te_db) not found.")
            print(f"!!! Please create the database before starting with this command:")
            print(f"\ncreatedb te_db")
        else:
            print("\n!!! There was a problem connecting to Postgres:\n{e}")
        sys.exit(1)

    #~ Check for disallowed characters in table name
    if args.table[0].isnumeric() or not re.match("^[a-zA-Z0-9_]+$", args.table):
        print("\n!!! Table names cannot start with a number, or include symbols except_underscores.")
        sys.exit(1)

    #~ Create a cursor object
    cursor = connection.cursor()

    create_table(
        args.table,
        connection,
        cursor)

    import_csv_to_pg_table(
        args.input,
        args.table,
        connection,
        cursor)

    table_details(
        args.table,
        cursor,
        connection)

    connection.close()


if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        print("OK, stopping.")
