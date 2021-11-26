#!/bin/sh
echo "Testing PG_querier_boots.py, just a moment..."
#~ four
echo "\n=== Testing combinations of four queries..."
echo "Testing customer + product + store + date"
python PG_querier_boots.py --customer 0874786793 --product 3921379 --store 6565 --date 20210215 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + product + store + daterange"
python PG_querier_boots.py --customer 0874786793 --product 3921379 --store 6565 --date 20210215 20210217 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + product + store + date, transaction to product JOIN"
python PG_querier_boots.py --customer 0874786793 --product 3921379 --store 6565 --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + product + store + daterange, transaction to product JOIN"
python PG_querier_boots.py --customer 0874786793 --product 3921379 --store 6565 --date 20210215 20210217 --join &>/dev/null && echo "OK" || echo "Failed";
#~ three
echo "\n=== Testing combinations of three queries..."
echo "Testing customer + product + store"
python PG_querier_boots.py --customer 0874786793 --product 3921379 --store 6565 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + product + date"
python PG_querier_boots.py --customer 0874786793 --product 3921379 --date 20210215 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + product + daterange"
python PG_querier_boots.py --customer 0874786793 --product 3921379 --date 20210215 20210217 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + store + date"
python PG_querier_boots.py --customer 0874786793 --store 6565 --date 20210215 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + store + daterange"
python PG_querier_boots.py --customer 0874786793 --store 6565 --date 20210215 20210217 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + product + store, transaction to product JOIN"
python PG_querier_boots.py --customer 0874786793 --product 3921379 --store 6565 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + product + date, transaction to product JOIN"
python PG_querier_boots.py --customer 0874786793 --product 3921379 --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + store + date, transaction to product JOIN"
python PG_querier_boots.py --customer 0874786793 --store 6565 --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + store + daterange, transaction to product JOIN"
python PG_querier_boots.py --customer 0874786793 --product 3921379 --date 20210215 20210217 --join &>/dev/null && echo "OK" || echo "Failed";
#~ two
echo "\n=== Testing combinations of two queries..."
echo "Testing customer + product"
python PG_querier_boots.py --customer 0874786793 --product 3921379 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + store"
python PG_querier_boots.py --customer 0874786793 --store 6565 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + date"
python PG_querier_boots.py --customer 0874786793 --date 20210215 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + daterange"
python PG_querier_boots.py --customer 0874786793 --date 20210215 20210217 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing product + store"
python PG_querier_boots.py --product 3921379 --store 6565 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing product + date"
python PG_querier_boots.py --product 3921379 --date 20210215 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing product + daterange"
python PG_querier_boots.py --product 3921379 --date 20210215 20210217 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing store + date"
python PG_querier_boots.py --store 6565 --date 20210215 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing store + daterange"
python PG_querier_boots.py --store 6565 --date 20210215 20210217&>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + product, transaction to product JOIN"
python PG_querier_boots.py --customer 0874786793 --product 3921379 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + store, transaction to product JOIN"
python PG_querier_boots.py --customer 0874786793 --store 6565 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + date, transaction to product JOIN"
python PG_querier_boots.py --customer 0874786793 --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + daterange, transaction to product JOIN"
python PG_querier_boots.py --customer 0874786793 --date 20210215 20210217 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing product + store, transaction to product JOIN"
python PG_querier_boots.py --product 3921379 --store 6565 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing product + date, transaction to product JOIN"
python PG_querier_boots.py --product 3921379 --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing product + daterange, transaction to product JOIN"
python PG_querier_boots.py --product 3921379 --date 20210215 20210217 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing store + date, transaction to product JOIN"
python PG_querier_boots.py --store 6565 --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing store + daterange, transaction to product JOIN"
python PG_querier_boots.py --store 6565 --date 20210215 20210217 --join &>/dev/null && echo "OK" || echo "Failed";

#~ one
echo "\n=== Testing combinations of one query..."
echo "Testing customer"
python PG_querier_boots.py --customer 0874786793 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing product"
python PG_querier_boots.py --product 3921379 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing store"
python PG_querier_boots.py --store 6565 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing date"
python PG_querier_boots.py --date 20210215 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing daterange"
python PG_querier_boots.py --date 20210215 20210217 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer, transaction to product JOIN"
python PG_querier_boots.py --customer 0874786793 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing product, transaction to product JOIN"
python PG_querier_boots.py --product 3921379 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing store, transaction to product JOIN"
python PG_querier_boots.py --store 6565 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing date, transaction to product JOIN"
python PG_querier_boots.py --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing daterange, transaction to product JOIN"
python PG_querier_boots.py --date 20210215 20210217 --join &>/dev/null && echo "OK" || echo "Failed";
