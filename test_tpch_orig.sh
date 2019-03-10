#!/bin/bash
for file in `ls -1 ./orig`
do
  ./flush_cache.sh
  (time impala-shell  -i cp-2.cloudera-test5.rdmaresearch-pg0.wisc.cloudlab.us:21000 -d tpch500g_parquet -f ./orig/$file) &> ./results/$file.out
done
