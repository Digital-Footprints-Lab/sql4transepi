# sql4transepi

  ![release](https://img.shields.io/badge/release-beta-brightgreen)
  [![GPLv3 license](https://img.shields.io/badge/licence-GPL_v3-blue.svg)](http://perso.crans.org/besson/LICENSE.html)
  ![DOI](https://img.shields.io/badge/DOI-TBC-blue.svg)

Python scripts for building and querying PostgreSQL databases. These are designed for working with databases of products and transactions in the context of transactional epidemiology.

## Getting Started

1. Copy the files in this repository to your machine, either through the [download](https://github.com/altanner/sql4transepi/archive/refs/heads/main.zip) link, or by cloning the repo:

`git clone https://github.com/altanner/sql4transepi.git`

and move into the repository folder:

`cd sql4transepi`

2. Install Postgres for the command line. You can follow a [instructions](https://www.postgresqltutorial.com/install-postgresql/) to do this here. There are guides for Linux, MacOS and Windows in that link.

3. Create a database called "TE_DB" to receive the incoming data. This can be done by running `createdb` (this is a postgres-installed command):

`createdb TE_DB`

If you get a `role does not exist` error, run this command to make yourself the owner of the database:

`sudo -u postgres createuser --superuser $USER`

4. Have Python >= 3.8 installed, and create a fresh virtual environment. This command creates a folder with clean Python binaries which we can then update to be streamlined for these scripts:

`python3 -m venv ./venv`

5. activate this clean Python with

`source ./venv/bin/activate`

(to exit this virtual environment, type `deactivate`)

6. finally, ask `pip` to install the `requirements.txt` file to get your libraries into this fresh Python

`pip install -r requirements.txt`

You will now be ready to use these scripts.


## Scripts

All scripts provide full help details by adding the flag `--help`, for example

`python CSV2PG_boots_card.py --help`

A status-reporting script is included, which can return technical details of the database, tables, and the connection to the working database:

`python PG_status.py`

#### • Data importing scripts 
All of the scripts named starting with `CSV2PG` import comma separated values files into Postgres. The data they can import are Boots Advantage loyalty cards, Boots product details (from the [scraper associated with this project](github.com/altanner/snax2)), Tescos Clubcard loyalty cards, as well as testing datasets - these are all run by specifying the file to import (the `my_data.csv` below will need to match the file you are importing):

```
python CSV2PG_boots_card.py -i my_data.csv
python CSV2PG_boots_scrape.py -i my_data.csv
python CSV2PG_tesco_card.py -i my_data.csv
python CSV2PG_foodproducts.py -i my_data.csv
```

These will report what they have done, and the status of the database after import.

#### • tesco_card_JSON2CSV.py 
Tescos loyalty card data is provided as nested JSON. This script creates a CSV, with one item per row, and adding a storeID, timestamp and a hash-generated customer ID to each transaction item. (Tescos cards data do not have complete card numbers, or any other customer-identifiers.)

```python tesco_card_JSON2CSV.py -i my_tesco_data210929.json```

#### • Database query scripts 
All scripts named starting with `PG_querier` are for returning data in the database in response to queries that you build by providing flags.



