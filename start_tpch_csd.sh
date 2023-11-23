#!/bin/bash

dev_name="/dev/nvme2n1"
mnt_name="/mnt/cslcsd"
csd_path="/mnt/fuse"

input=0
query=1
thread=1
# thread=4

if [ $# -gt 1 ]
then
    #echo "Parameter set for Q01"
    thread=$2
fi
if [ $# -gt 0 ]
then
    #echo "Parameter set for Q01"
    query=$1
fi

export TPCH_INPUT_DATA_DIR=$mnt_name/tpch
export TPCH_FILTER_DATA_DIR=$csd_path
export LD_LIBRARY_PATH=$PWD/lib


pushd ../
export PATH=$PATH:$PWD/spark-3.4.0-bin-hadoop3/bin
popd

if [ $query -eq 12 ]
then
    spark-submit --conf spark.default.parallelism=1 --conf spark.sql.files.maxPartitionBytes=1GB --num-executors 1 --executor-cores 1 --class "tpch.TpchQuery" target/scala-2.12/spark-tpc-h-queries_2.12-1.0.jar true $query
elif [ $query -eq 6 ]
then
    echo "EQ 6"
    spark-submit --conf spark.default.parallelism=1 --conf spark.sql.files.maxPartitionBytes=1GB --num-executors 1 --executor-cores 1 --class "tpch.TpchQuery" target/scala-2.12/spark-tpc-h-queries_2.12-1.0.jar true $query
elif [ $query -eq 14 ]
then
    echo "EQ 6"
    spark-submit --conf spark.default.parallelism=1 --conf spark.sql.files.maxPartitionBytes=1GB --num-executors 1 --executor-cores 1 --class "tpch.TpchQuery" target/scala-2.12/spark-tpc-h-queries_2.12-1.0.jar true $query
elif [ $query -eq 15 ]
then
    echo "EQ 6"
    spark-submit --conf spark.default.parallelism=1 --conf spark.sql.files.maxPartitionBytes=1GB --num-executors 1 --executor-cores 1 --class "tpch.TpchQuery" target/scala-2.12/spark-tpc-h-queries_2.12-1.0.jar true $query
elif [ $query -eq 19 ]
then
    echo "EQ 19"
    spark-submit --conf spark.default.parallelism=1 --conf spark.sql.files.maxPartitionBytes=1GB --num-executors 1 --executor-cores 1 --class "tpch.TpchQuery" target/scala-2.12/spark-tpc-h-queries_2.12-1.0.jar true $query    
else
    spark-submit --conf spark.default.parallelism=$thread --conf spark.sql.files.maxPartitionBytes=1GB --num-executors $thread --executor-cores $thread --class "tpch.TpchQuery" target/scala-2.12/spark-tpc-h-queries_2.12-1.0.jar true $query
    #spark-submit --conf spark.default.parallelism=1 --conf spark.sql.files.maxPartitionBytes=1GB --num-executors 1 --executor-cores 1 --class "tpch.TpchQuery" target/scala-2.12/spark-tpc-h-queries_2.12-1.0.jar true $query
fi

#spark-submit --class "tpch.TpchQuery" target/scala-2.12/spark-tpc-h-queries_2.12-1.0.jar true $query