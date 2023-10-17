#!/bin/bash

pushd /home/csl/ilkueon/linux_for_csd/nvme_csd/tools/virt
./init_nvmev.sh 
popd

dev_name="/dev/nvme2n1"
mnt_name="/mnt/cslcsd"
#mnt_name="/mnt/test/hello

sudo umount $mnt_name
sudo mkdir $mnt_name
sudo mkfs.ext4 $dev_name
sudo mount $dev_name $mnt_name

sudo chmod 777 $mnt_name
sudo mkdir $mnt_name/tpch
sudo cp ./dbgen/*.tbl $mnt_name/tpch/
sudo chmod 777 $mnt_name/tpch
sudo chmod 777 $mnt_name/tpch/*

sudo sync
sudo sync
sudo sync
sudo sync
sudo echo 1 > /proc/sys/vm/drop_caches
sudo echo 2 > /proc/sys/vm/drop_caches
sudo echo 3 > /proc/sys/vm/drop_caches
