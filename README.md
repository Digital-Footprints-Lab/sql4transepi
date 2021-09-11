# sql4transepi

  ![release](https://img.shields.io/badge/release-beta-brightgreen)
  [![GPLv3 license](https://img.shields.io/badge/licence-GPL_v3-blue.svg)](http://perso.crans.org/besson/LICENSE.html)
  ![DOI](https://img.shields.io/badge/DOI-TBC-blue.svg)

A set of Python scripts for building and querying PostgreSQL databases.

These are designed for querying databases of products and transactions for transactional epidemiology.

## Guide

1. Copy the files in this repository to your machine, either through the [download](https://github.com/altanner/sql4transepi/archive/refs/heads/main.zip) link, or by cloning the repo:

`git clone https://github.com/altanner/sql4transepi.git`

2. Install Postgres for the command line. You can follow a [instructions](https://www.postgresqltutorial.com/install-postgresql/) to do this here. There are guides for Linux, MacOS and Windows in that link.

3. Create a database to receive the incoming data. This can be done by running `createdb` (this is a postgres-installed command), for example

`createdb database_5`

If you get a `role does not exist` error, run this command to make yourself the owner of the database:

`sudo -u postgres createuser --superuser $USER`

4. Have Python >= 3.8 installed, and install the `requirements.txt` file to acquire the dependencies

`pip install -r requirements.txt`

You will now be ready to use these scripts.

### csv2pg.py

This imports a Comma Separated Values file to a Postgres table. 

Now that a database is created, we can bring the CSV file into a table. Run the Python script declaring the recieving DB and giving a name to your table, for example

`python3 csv2pg.py -d database_5 -t table_1 -f records.csv`

`-d` is the flag prior to your DB name, `-t` for table name, and `-f` will be the path to your incoming CSV file (usually just the name of the file, if the file is in the working folder). After import, the script will give you some summary details of what the table now contains.

### pg_querier.py

pg_querier.py runs user-defined queries against a database, and can return raw records or summaries in the form of counts, totals and averages. The syntax for using the querier is

`python3 pg_querier.py -d [your DB name] -t [your table name] [your queries as flags]`

The available queries will be listed on an error, or you can run `python3 pg_querier --help`

```  -h, --help            show this help message and exit
  --details             Provide DB and table information.
  -d DB, --db DB        The name of the DB to query.
  -t TABLE, --table TABLE
                        The name of the table to query.
  --customer CUSTOMER   Customer code to query. Format: CUST0123456789
  --product PRODUCT     Product code to query. Format: PRD0123456
  --hour HOUR           Shop hour to query (24 hour, 2 digits). Format: HH
  --date DATE           Shop date to query. Format: YYYYMMDD
  --week WEEK           Shop week (of year) to query. Format: YYYYNN
  --weekday WEEKDAY     Shop weekday (1-7) to query. Format: N
  --basket BASKET       Basket ID. Format: 123450123456789
  --count               Return total record counts.
  --spend               Return total spend for the query.

Example: python pg_querier.py -d database1 -t table1 --customer CUST001 --date 20180621 --spend
```

The above example is asking for the total spend from customer identified as `CUST001` on the 21st of June 2018. Without the flag `--spend`, raw records will be output, which can then be piped into other commands, or saved into a file. Below are some more usage examples:

Provide some information on the dimensions and content of a table:

`python3 pg_querier.py -d database1 -t table1 --details`

Return all the records for transactions of a particular product, in a particular week:

`python3 pg_querier.py -d database1 -t table1 --product PRD0900684 --week 200626`

Return the total item transaction count for a customer, in a particular week:

`python3 pg_querier.py -d database1 -t table1 --customer CUST0000472158 --week 200626 --count`
