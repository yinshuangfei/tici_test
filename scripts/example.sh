# Run auto cmd in background
nohup ./main.py auto --host test-infra-tunnel.pingcap.net --port 42309 --count 5000 --row-limit 100000 --freshness-batch 10000 --table-offset 0 1>do.log 2>do_err.log &

# Run tow different task at tow nodes, 10000 tables
## Node-1
nohup ./main.py auto --host test-infra-tunnel.pingcap.net --port 42309 --count 5000 --row-limit 100000 --freshness-batch 10000 --table-offset 0 1>do.log 2>do_err.log &
## Node-2
nohup ./main.py auto --host test-infra-tunnel.pingcap.net --port 42309 --count 5000 --row-limit 100000 --freshness-batch 10000 --table-offset 5000 1>do.log 2>do_err.log &

# Insert rows to exists tables
./main.py insert-data --host test-infra-tunnel.pingcap.net --port 42309 --count 5000 --row-limit 100000 --freshness-batch 10000
./main.py insert-data --host test-infra-tunnel.pingcap.net --port 42006 --count 5000 --row-limit 100000 --freshness-batch 10000 --table-offset 5000

# Insert rows to exists tables without freshness check
./main.py insert-data --host test-infra-tunnel.pingcap.net --port 42309 --count 5000 --row-limit 100000 --freshness-batch 10000 --no-freshness

# Run cmd in vpc 
./main.py insert-data --host tc-tidb --port 4000 --count 2000 --row-limit 100000 --freshness-batch 100000 --table-offset 3000

# Network Metrics
## check rto
ss -ti dport = 42006 | tail
