#!/bin/bash
for i in {1..1000}
do
    echo "正在统计表 test.hdfs_log_$i ..."
    mysql -h test-infra-tunnel.pingcap.net -P40319 -uroot -e "SELECT '$i' AS table_idx, COUNT(*) FROM test.hdfs_log_$i;"
done
