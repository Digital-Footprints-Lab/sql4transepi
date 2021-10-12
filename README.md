# sql4transepi

  ![release](https://img.shields.io/badge/release-beta-brightgreen)
  [![GPLv3 license](https://img.shields.io/badge/licence-GPL_v3-blue.svg)](http://perso.crans.org/besson/LICENSE.html)
  ![DOI](https://img.shields.io/badge/DOI-TBC-blue.svg)

Python scripts for building and querying PostgreSQL databases. These are designed for working with databases of products and transactions in the context of transactional epidemiology.

## User Guide

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

### CSV2PG.py

These scripts import Comma Separated Values files into Postgres. Each of these is tailored for different types of CSV, for example loyalty card customer transaction data, or retailer product information data, and named as such. 

Run the Python script, using the flags `-d -t -i` to specify the database name, table name and input CSV file name respectively.

`python3 tescos_CSV2PG.py -d database_5 -t table_1 -i tesco_loyalty_card.csv`

After import, the script will give you some summary details of what the table now contains.

### pg_querier.py

pg_querier.py runs user-defined queries against a database, and can return raw records or summaries in the form of counts, totals and averages. The syntax for using the querier is

`python3 pg_querier.py -d [your DB name] -t [your table name] [your queries as flags]`

The available queries will be listed on an error, or you can run `python3 pg_querier --help`

```
  -h, --help            show this help message and exit
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
