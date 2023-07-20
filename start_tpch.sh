#!/bin/bash

dev_name="/dev/nvme2n1"
mnt_name="/mnt/cslcsd"

input=0
query=1
if [ $# -gt 0 ]
then
    #echo "Parameter set for no compile"
    input=$1
fi

if [ $# -gt 1 ]
then
    #echo "Parameter set for Q01"
    query=$2
fi

if [ $input -gt 1 ]
then
    sudo umount $mnt_name
    sudo mkdir $mnt_name
    sudo mkfs.ext4 $dev_name
    sudo mount $dev_name $mnt_name

    sudo chmod 777 $mnt_name
    sudo mkdir $mnt_name/tpch
    sudo cp ./dbgen/*.tbl $mnt_name/tpch/
fi
export TPCH_INPUT_DATA_DIR=$mnt_name/tpch
export LD_LIBRARY_PATH=$PWD/lib

if [ $input -gt 0 ]
then
    echo "== Compile source =="
    sbt package || exit
fi

sudo sync
sudo echo 1 > /proc/sys/vm/drop_caches

if [ $input -ne 10 ]
then
    pushd ../
    export PATH=$PATH:$PWD/spark-3.4.0-bin-hadoop3/bin
    popd

    spark-submit --conf spark.default.parallelism=1 --conf spark.sql.files.maxPartitionBytes=1GB --num-executors 1 --executor-cores 1 --class "tpch.TpchQuery" target/scala-2.12/spark-tpc-h-queries_2.12-1.0.jar true $query
fi