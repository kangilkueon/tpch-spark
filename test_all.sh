#!/bin/bash

echo "" > ./tpch_execution_times.txt
# 반복 횟수 설정
count=5
queries="1 3 4 6 7 10 12 14 15 19 20"
mode=0
thread=1
script="start_tpch_csd.sh"
#script="start_tpch.sh"

echo "## CSD_$mode (thread $thread) ##" >> ./tpch_execution_times.txt
echo "" >> ./tpch_execution_times.txt
# 반복문 시작
for q in $queries
do
    for ((i=1; i<=$count; i++))
    do
        echo "[$q] 반복 실행 횟수: $i (Thread:$thread, Query:$q)"

        ./perf.sh
        ./run_fuse.sh $mode $q $thread &
        fuse_pid=$!
 
        ./$script $q $thread &        
        tpch_pid=$!
        wait $tpch_pid
        kill $fuse_pid
        echo "TPCH_PID : $tpch_pid, FUSE_PID: $fuse_pid"
    done
done

thread=4
echo "" >> ./tpch_execution_times.txt
echo "## CSD_$mode (thread $thread) ##" >> ./tpch_execution_times.txt
echo "" >> ./tpch_execution_times.txt
# 반복문 시작
for q in $queries
do
    for ((i=1; i<=$count; i++))
    do
        echo "[$q] 반복 실행 횟수: $i"
        ./perf.sh
        ./run_fuse.sh $mode $q $thread &
        fuse_pid=$!

        ./$script $q $thread &        
        tpch_pid=$!
        wait $tpch_pid
        kill $fuse_pid

        echo "TPCH_PID : $tpch_pid, FUSE_PID: $fuse_pid"
    done
done