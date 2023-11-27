#!/bin/bash

temp_file="size_data_temp.txt"
output_file="size_data.txt"
t=1

echo "Measure size start" > $temp_file
queries="1 3 4 6 7 10 12 14 15 19 20"
for q in $queries
do
    ./run_fuse.sh 0 $q $t >> $temp_file &

    sleep 2
    echo "head -n 1 /mnt/fuse/lineitem.tbl"
    head -n 1 /mnt/fuse/lineitem.tbl 

    sleep 20
    umount /mnt/fuse
done


t=4
for q in $queries
do
    ./run_fuse.sh 0 $q $t >> $temp_file &

    sleep 2
    echo "head -n 1 /mnt/fuse/lineitem.tbl"
    head -n 1 /mnt/fuse/lineitem.tbl 

    sleep 20
    umount /mnt/fuse
done

grep "Filter size error" $temp_file | awk -F '[[:space:]:,]+' '{print "            {" $3 ", " $8 "},"}' > $output_file