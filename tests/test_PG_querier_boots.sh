#!/bin/sh
fails=0;
passes=0;
echo "Testing PG_querier_boots.py, just a moment..."

#~ four queries
echo "\n=== Testing combinations of four queries..."
echo "Testing customer + product + store + date"
python PG_querier_boots.py --customer 0874786793 --product 3921379 --store 6565 --date 20210215 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + product + store + daterange"
python PG_querier_boots.py --customer 0874786793 --product 3921379 --store 6565 --date 20210215 20210217 &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + product + store + date, transaction <=> product JOIN"
python PG_querier_boots.py --customer 0874786793 --product 3921379 --store 6565 --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + product + store + daterange, transaction <=> product JOIN"
python PG_querier_boots.py --customer 0874786793 --product 3921379 --store 6565 --date 20210215 20210217 --join &>/dev/null && echo "OK" || echo "Failed";

#~ three queries
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
echo "Testing customer + product + store, transaction <=> product JOIN"
python PG_querier_boots.py --customer 0874786793 --product 3921379 --store 6565 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + product + date, transaction <=> product JOIN"
python PG_querier_boots.py --customer 0874786793 --product 3921379 --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + store + date, transaction <=> product JOIN"
python PG_querier_boots.py --customer 0874786793 --store 6565 --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + store + daterange, transaction <=> product JOIN"
python PG_querier_boots.py --customer 0874786793 --product 3921379 --date 20210215 20210217 --join &>/dev/null && echo "OK" || echo "Failed";

#~ two queries
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
echo "Testing customer + product, transaction <=> product JOIN"
python PG_querier_boots.py --customer 0874786793 --product 3921379 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + store, transaction <=> product JOIN"
python PG_querier_boots.py --customer 0874786793 --store 6565 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + date, transaction <=> product JOIN"
python PG_querier_boots.py --customer 0874786793 --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing customer + daterange, transaction <=> product JOIN"
python PG_querier_boots.py --customer 0874786793 --date 20210215 20210217 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing product + store, transaction <=> product JOIN"
python PG_querier_boots.py --product 3921379 --store 6565 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing product + date, transaction <=> product JOIN"
python PG_querier_boots.py --product 3921379 --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing product + daterange, transaction <=> product JOIN"
python PG_querier_boots.py --product 3921379 --date 20210215 20210217 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing store + date, transaction <=> product JOIN"
python PG_querier_boots.py --store 6565 --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing store + daterange, transaction <=> product JOIN"
python PG_querier_boots.py --store 6565 --date 20210215 20210217 --join &>/dev/null && echo "OK" || echo "Failed";

#~ one query
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
echo "Testing customer, transaction <=> product JOIN"
python PG_querier_boots.py --customer 0874786793 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing product, transaction <=> product JOIN"
python PG_querier_boots.py --product 3921379 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing store, transaction <=> product JOIN"
python PG_querier_boots.py --store 6565 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing date, transaction <=> product JOIN"
python PG_querier_boots.py --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
echo "Testing daterange, transaction <=> product JOIN"
python PG_querier_boots.py --date 20210215 20210217 --join &>/dev/null && echo "OK" || echo "Failed";
