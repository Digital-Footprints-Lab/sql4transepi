echo "Testing PG_querier_boots.py, just a moment..."
#~ four
echo "Testing combinations of four queries..."
python PG_querier_boots.py --customer 0874786793 --product 3921379 --store 6565 --date 20210215 20210217 &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --customer 0874786793 --product 3921379 --store 6565 --date 20210215 &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --customer 0874786793 --product 3921379 --store 6565 --date 20210215 20210217 --join &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --customer 0874786793 --product 3921379 --store 6565 --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
#~ three
echo "Testing combinations of three queries..."
python PG_querier_boots.py --customer 0874786793 --product 3921379 --store 6565 &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --customer 0874786793 --product 3921379 --date 20210215 &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --customer 0874786793 --product 3921379 --date 20210215 20210217 &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --customer 0874786793 --store 6565 --date 20210215 &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --customer 0874786793 --product 3921379 --store 6565 --join &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --customer 0874786793 --product 3921379 --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --customer 0874786793 --product 3921379 --date 20210215 20210217 --join &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --customer 0874786793 --store 6565 --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
#~ two
echo "Testing combinations of two queries..."
python PG_querier_boots.py --customer 0874786793 --product 3921379 &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --customer 0874786793 --store 6565 &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --customer 0874786793 --date 20210215 &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --customer 0874786793 --date 20210215 20210217 &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --product 3921379 --store 6565 &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --product 3921379 --date 20210215 &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --store 6565 --date 20210215 &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --customer 0874786793 --product 3921379 --join &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --customer 0874786793 --store 6565 --join &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --customer 0874786793 --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --customer 0874786793 --date 20210215 20210217 --join &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --product 3921379 --store 6565 --join &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --product 3921379 --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --store 6565 --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
#~ one
echo "Testing combinations of one query..."
python PG_querier_boots.py --customer 0874786793 &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --product 3921379 &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --store 6565 &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --date 20210215 &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --date 20210215 20210217 &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --customer 0874786793 --join &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --product 3921379 --join &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --store 6565 --join &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --date 20210215 --join &>/dev/null && echo "OK" || echo "Failed";
python PG_querier_boots.py --date 20210215 20210217 --join &>/dev/null && echo "OK" || echo "Failed";
