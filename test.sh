#!/bin/bash

# 반복 횟수 설정
count=5
queries="1 3 4 6 7 10 12 14 15 19 20"
thread=4
mode=1
# 반복문 시작
for q in $queries
do
    for ((i=1; i<=$count; i++))
    do
        echo "[$q] 반복 실행 횟수: $i"
        ./perf.sh
        ./run_fuse.sh $mode $q $thread &
        fuse_pid=$1

        #./start_tpch.sh $q $thread &   
        ./start_tpch_csd.sh $q $thread &        
        tpch_pid=$!
        wait $tpch_pid
        kill $fuse_pid

        sleep 5
    done
done
