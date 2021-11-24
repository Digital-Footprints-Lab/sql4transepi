
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
import PG_status


def args_setup():

    parser = argparse.ArgumentParser(
        description="Postgres DB Importer: Boots Advantage Loyalty Cards.",
        epilog="Example: python boots_card_CSV2PG.py -i boots_card.csv")
    parser.add_argument(
        "-i", "--input", type=argparse.FileType("r"), default=sys.stdin,
        metavar="PATH",
        help="CSV file to import.")

    args = parser.parse_args()

    if len(sys.argv) < 2:
        parser.print_help(sys.stderr)
        sys.exit(1)

    return parser, args


def create_table(connection, cursor):

    """Create a table consistent with CSVs returned from Boot
    after an information request.

    Fields which you might think would be INT are VARCHAR because
    they sometimes start with 0s, which we want to preserve.
    """

    sql = Template("""
        CREATE TABLE IF NOT EXISTS $table (
        ID VARCHAR,
        DATE2 DATE,
        TIME3 TIME,
        STORE VARCHAR,
        PAYMENT VARCHAR,
        STAFF_DISCOUNT_CARD_NUMBER VARCHAR,
        ITEM_CODE VARCHAR,
        ITEM_DESCRIPTION VARCHAR,
        POINTS_ADJUSTMENT INT,
        POINTS_ITEM REAL,
        UNITS INT,
        SPEND MONEY,
        DISCOUNT REAL);""")

    try:
        cursor.execute(sql.substitute(table="boots_transactions"))
        connection.commit()
    except Exception as e:
        print(e)


def import_csv_to_pg_table(
    csv,
    csv_original_name,
    connection,
    cursor):

    """Imports a CSV with columns consistent with Boots loyalty card CSV columns"""

    print(f"\nImporting Boots Card {csv} to Postgres table 'boots_transactions', just a moment...")

    dirname = os.path.dirname(__file__)
    csv_path = os.path.join(dirname, csv)

    sql = Template("""
        COPY $table (
        ID,
        DATE2,
        TIME3,
        STORE,
        PAYMENT,
        STAFF_DISCOUNT_CARD_NUMBER,
        ITEM_CODE,
        ITEM_DESCRIPTION,
        POINTS_ADJUSTMENT,
        POINTS_ITEM,
        UNITS,
        SPEND,
        DISCOUNT)
        FROM '$csv_path' CSV HEADER;""")

    try:
        cursor.execute(sql.substitute(table="boots_transactions", csv_path=csv_path))
        connection.commit()
        print(f"\nOK, {csv} imported.")
    except Exception as e:
        print(f"\n!!! Import failed: {csv_original_name} is not consistent with table fields.")
        print(f"!!! The csv format might not be correct?")
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
    connection,
    cursor):

    """
    Return some information about the Boots card import to Postgres.
    """

    sql_record_count = Template("""
        SELECT COUNT(*)
        FROM $table;""")
    sql_column_count = Template("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name='$table';""")
    sql_id_count = Template("""
        SELECT COUNT (DISTINCT ID) FROM $table;""")
    sql_item_count = Template("""
        SELECT COUNT (DISTINCT ITEM_CODE) FROM $table;""")
    sql_date_count = Template("""
        SELECT COUNT (DISTINCT DATE2) FROM $table;""")

    try:
        cursor.execute(sql_record_count.substitute(table="boots_transactions"))
        record_count = cursor.fetchall()
        cursor.execute(sql_column_count.substitute(table="boots_transactions"))
        column_count = cursor.fetchall()
        cursor.execute(sql_id_count.substitute(table="boots_transactions"))
        id_count = cursor.fetchall()
        cursor.execute(sql_item_count.substitute(table="boots_transactions"))
        item_count = cursor.fetchall()
        cursor.execute(sql_date_count.substitute(table="boots_transactions"))
        date_count = cursor.fetchall()
        print(f"\nboots_transactions details:\nRecords:       {record_count[0][0]}")
        print(f"Column count:  {column_count[0][0]}")
        print(f"Customer IDs:  {id_count[0][0]}")
        print(f"Items:         {item_count[0][0]}")
        print(f"Shop dates:    {date_count[0][0]}")
    except Exception as e:
        print("\nNo boots_transactions table present.")


def main():

    if len(sys.argv) < 2:
        print("\nPostgres DB Importer: Boots Advantage Loyalty Cards.")
        print("Please provide an input file, for example:")
        print("\npython boots_card_CSV2PG.py -i card4374832.csv")
        sys.exit(1)

    parser, args = args_setup()

    #~ Create connection using psycopg2
    connection, cursor = PG_status.connect_to_postgres(db_config)

    #~ Boots cards come as UTF16 TSV: detect and convert to UTF8 CSV
    outfile_name = args.input.name.replace(".csv", ".utf-8.csv")
    with codecs.open(args.input.name, "rb") as input_file:
        encoding = chardet.detect(input_file.read())
    if encoding["encoding"] == "UTF-16":
        print(f"\nInput file {args.input.name} detected as UTF-16: converting to UTF-8.")
        with codecs.open(args.input.name, "r", encoding="utf-16") as input_file:
            card_file_contents = input_file.read()
            utf8_card_file_contents = card_file_contents.replace("\t", ",")
            with codecs.open(outfile_name, "w", encoding="utf-8") as output_file:
                output_file.write(utf8_card_file_contents)
    else:
        print(f"\nInput file {args.input.name} detected as UTF-8.")
        with codecs.open(args.input.name, "r") as input_file:
            card_file_contents = input_file.read()
            utf8_card_file_contents = card_file_contents.replace("\t", ",")
            with codecs.open(outfile_name, "w", encoding="utf-8") as output_file:
                output_file.write(utf8_card_file_contents)

    create_table(
        connection,
        cursor)

    import_csv_to_pg_table(
        outfile_name,
        args.input.name,
        connection,
        cursor)

    table_details(
        connection,
        cursor)

    connection.close()


if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        print("OK, stopping.")
