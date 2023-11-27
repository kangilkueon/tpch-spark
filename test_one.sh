#!/bin/bash

# 반복 횟수 설정
count=1
mode=$1
thread=$2
queries=$3

script="start_tpch_csd.sh"
#script="start_tpch.sh"

for ((i=1; i<=$count; i++))
do
    echo "[$q] 반복 실행 횟수: $i"
    ./perf.sh
    ./run_fuse.sh $mode $queries $thread &
    fuse_pid=$1

    ./$script $queries $thread &        
    tpch_pid=$!
    wait $tpch_pid
    kill $fuse_pid
done

umount /mnt/fuse