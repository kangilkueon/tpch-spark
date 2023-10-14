#!/bin/bash

dev_name="/dev/nvme2n1"
mnt_name="/mnt/cslcsd"
#mnt_name="/mnt/fuse"

input=0
query=1

if [ $# -gt 0 ]
then
    #echo "Parameter set for Q01"
    query=$1
fi

export TPCH_INPUT_DATA_DIR=$mnt_name/tpch
export LD_LIBRARY_PATH=$PWD/lib


pushd ../
export PATH=$PATH:$PWD/spark-3.4.0-bin-hadoop3/bin
popd

spark-submit --conf spark.default.parallelism=1 --conf spark.sql.files.maxPartitionBytes=1GB --num-executors 1 --executor-cores 1 --class "tpch.TpchQuery" target/scala-2.12/spark-tpc-h-queries_2.12-1.0.jar true $query
#spark-submit --class "tpch.TpchQuery" target/scala-2.12/spark-tpc-h-queries_2.12-1.0.jar true $query