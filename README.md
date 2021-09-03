# sql4transepi

A set of Python scripts for building, verifying and querying PostgreSQL databases.

These are designed for querying databases of products and transactions for transactional epidemiology.

## Guide

1. Install Postgres for the command line. You can follow a [guide](https://www.postgresqltutorial.com/install-postgresql/) here. 

2. Have Python >= 3.8 installed, and import the `requirements.txt` file to acquire the dependencies

`pip install -r requirements.txt`

You will now be ready to use the scripts here.

### csv2pg.py

This imports a Comma Separated Values file to a Postgres table. You will first need to create a database to receive the incoming data. This can be done by running `createdb` (this is a postgres-installed command), for example

`createdb database_5`

Now that a databse is created, we can bring the CSV file into a table. Run the Python script declaring the recieving DB and giving a name to your table, for example

`python3 csv2pg.py -d database_5 -t table_1 -f records.csv`

`-d` is the flag prior to your DB name, `-t` for table name, and `-f` will be the path to your incoming CSV file (usually just the name of the file, if the file is in the working folder). After import, the script will give you some summary details of what the table now contains.

### pg_querier.py
