
#~ Standard library imports
import sys
import os
import re
import argparse
from string import Template
import csv

#~ 3rd party imports
import pandas as pd
import psycopg2
from psycopg2 import Error

#~ local imports
import db_config
import PG_ops

def args_setup():

    parser = argparse.ArgumentParser(
        description="Postgres DB Importer: Foodproducts CSV Dataset.",
        epilog="Example: python foodproducts_CSV2PG.py -i foodproducts.csv")
    parser.add_argument(
        "-i", "--input", type=argparse.FileType("r"), default=sys.stdin,
        metavar="PATH", required=True,
        help="CSV file to import.")

    args = parser.parse_args()

    return parser, args


def create_table(connection, cursor):

    """Create a table consistent with the column names
    for the food example datasets"""

    sql = Template("""
        CREATE TABLE IF NOT EXISTS $table (
        x_id INT UNIQUE,
        x_descr VARCHAR,
        x_marketingdescr VARCHAR,
        x_tilldescr VARCHAR,
        x_friendlydescr VARCHAR,
        x_productdescription VARCHAR,
        x_brand VARCHAR,
        x_ingredients VARCHAR,
        x_energyserv FLOAT,
        x_fatserv FLOAT,
        x_saturatesserv FLOAT,
        x_saltserv FLOAT,
        x_sugarsserv FLOAT,
        x_proteinserv FLOAT,
        x_carbohydrateserv FLOAT,
        x_fibreserv FLOAT,
        x_energyunit FLOAT,
        x_fatunit FLOAT,
        x_saturatesunit FLOAT,
        x_saltunit FLOAT,
        x_sugarsunit FLOAT,
        x_proteinunit FLOAT,
        x_carbohydrateunit FLOAT,
        x_fibreunit FLOAT,
        x_allergens VARCHAR,
        y_category_id VARCHAR,
        l1y_division VARCHAR,
        l2y_group VARCHAR,
        l3y_department VARCHAR,
        l4y_class VARCHAR,
        l5y_subclass VARCHAR);""")

    try:
        cursor.execute(sql.substitute(table="food_products"))
        connection.commit()
    except Exception as e:
        print(e)

    #~ build a temp table to deal with duplicates
    sql = Template("""
        CREATE TABLE IF NOT EXISTS temp_table (
        x_id INT,
        x_descr VARCHAR,
        x_marketingdescr VARCHAR,
        x_tilldescr VARCHAR,
        x_friendlydescr VARCHAR,
        x_productdescription VARCHAR,
        x_brand VARCHAR,
        x_ingredients VARCHAR,
        x_energyserv FLOAT,
        x_fatserv FLOAT,
        x_saturatesserv FLOAT,
        x_saltserv FLOAT,
        x_sugarsserv FLOAT,
        x_proteinserv FLOAT,
        x_carbohydrateserv FLOAT,
        x_fibreserv FLOAT,
        x_energyunit FLOAT,
        x_fatunit FLOAT,
        x_saturatesunit FLOAT,
        x_saltunit FLOAT,
        x_sugarsunit FLOAT,
        x_proteinunit FLOAT,
        x_carbohydrateunit FLOAT,
        x_fibreunit FLOAT,
        x_allergens VARCHAR,
        y_category_id VARCHAR,
        l1y_division VARCHAR,
        l2y_group VARCHAR,
        l3y_department VARCHAR,
        l4y_class VARCHAR,
        l5y_subclass VARCHAR);""")

    try:
        cursor.execute(sql.substitute(table="food_products"))
        connection.commit()
    except Exception as e:
        print(e)


def import_csv_to_pg_table(
    csv,
    connection,
    cursor):

    """Imports a CSV with columns named from the foodproducts.csv
    Tesco example dataset"""

    print(f"Importing {csv.name} to Postgres table 'food_products', just a moment...")

    dirname = os.path.dirname(__file__)
    csv_path = os.path.join(dirname, csv.name)

    sql = Template("""
        COPY temp_table (
        x_id,
        x_descr,
        x_marketingdescr,
        x_tilldescr,
        x_friendlydescr,
        x_productdescription,
        x_brand,
        x_ingredients,
        x_energyserv,
        x_fatserv,
        x_saturatesserv,
        x_saltserv,
        x_sugarsserv,
        x_proteinserv,
        x_carbohydrateserv,
        x_fibreserv,
        x_energyunit,
        x_fatunit,
        x_saturatesunit,
        x_saltunit,
        x_sugarsunit,
        x_proteinunit,
        x_carbohydrateunit,
        x_fibreunit,
        x_allergens,
        y_category_id,
        l1y_division,
        l2y_group,
        l3y_department,
        l4y_class,
        l5y_subclass)
        FROM '$csv_path' CSV HEADER;""")

    try:
        cursor.execute(sql.substitute(csv_path=csv_path))
        connection.commit()
        print(f"\n{csv.name} imported to staging area, removing duplicates...")
    except Exception as e:
        print(e)

    #~ then INSERT to true table, rejecting dups on productid
    sql = Template("""
        INSERT INTO $table
        SELECT * FROM temp_table
        ON CONFLICT DO NOTHING;""")

    try:
        cursor.execute(sql.substitute(table="food_products", csv_path=csv_path))
        connection.commit()
        print(f"\nOK, {csv.name} imported.")
        #~ remove the temp table
        cursor.execute("""DROP TABLE IF EXISTS temp_table;""")
        connection.commit()
    except Exception as e:
        print(f"\n!!! Import failed: {csv.name} is not consistent with table fields.")
        print(f"!!! The csv format might not be correct?")
        sys.exit(1)


def db_details(
    connection,
    cursor):

    """Return some information about the DB after scrape import to Postgres.
    """

    sql_record_count = Template("""
        SELECT COUNT(*)
        FROM $table;""")
    sql_column_count = Template("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name='$table';""")
    sql_product_count = Template("""
        SELECT COUNT (DISTINCT x_id) FROM $table;""")
    sql_department_count = Template("""
        SELECT COUNT (DISTINCT l3y_department) FROM $table;""")
    sql_class_count = Template("""
        SELECT COUNT (DISTINCT l4y_class) FROM $table;""")

    try:
        cursor.execute(sql_record_count.substitute(table="food_products"))
        record_count = cursor.fetchall()
        cursor.execute(sql_column_count.substitute(table="food_products"))
        column_count = cursor.fetchall()
        cursor.execute(sql_product_count.substitute(table="food_products"))
        product_count = cursor.fetchall()
        cursor.execute(sql_department_count.substitute(table="food_products"))
        department_count = cursor.fetchall()
        cursor.execute(sql_class_count.substitute(table="food_products"))
        class_count = cursor.fetchall()
        print(f"\nfood_products details:\nRecords:     {record_count[0][0]}")
        print(f"Columns:     {column_count[0][0]}")
        print(f"Products:    {product_count[0][0]}")
        print(f"Classes:     {class_count[0][0]}")
        print(f"Departments: {department_count[0][0]}")
    except Exception as e:
        print(e)


def main():

    if len(sys.argv) < 2:
        print("\nPostgres DB Importer: Example file 'foodproducts.csv'.")
        print("Please provide an input file, for example:")
        print("\npython foodproducts_CSV2PG.py -i foodproducts.csv")
        sys.exit(1)

    parser, args = args_setup()

    #~ Create connection using psycopg2
    connection, cursor = PG_ops.connect_to_postgres(db_config)

    create_table(
        connection,
        cursor)

    import_csv_to_pg_table(
        args.input,
        connection,
        cursor)

    db_details(
        connection,
        cursor)

    connection.close()


if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        print("OK, stopping.")
