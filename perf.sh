#!/bin/bash

for c in {0..23}
do
	sudo cpufreq-set -g  userspace
	sudo cpufreq-set --max 2.1GHz -c $c
	sudo cpufreq-set --min 2.1GHz -c $c
done

sudo grep . /sys/devices/system/cpu/cpu*/cpufreq/cpuinfo_cur_freq
# 33,35,37,29,31,39,21,19,17
ccsd=(9 13 17 21)
clock=800MHz
for c in "${ccsd[@]}"
do
	sudo cpufreq-set -g  userspace
	sudo cpufreq-set --max $clock -c $c
	sudo cpufreq-set --min $clock -c $c
	#sudo cpufreq-set -f 800MHz -c $c
	#sudo cpufreq-set --max 2.1GHz -c $c
	#sudo cpufreq-set --min 2.1GHz -c $c
done
sudo grep . /sys/devices/system/cpu/cpu*/cpufreq/cpuinfo_cur_freq


CPU_AFFINITY=0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38

echo "Set CPU AFFINITY" $CPU_AFFINITY
taskset -p -c $CPU_AFFINITY $$ &> /dev/null
sleep 1 
# 5 -> 29
# 29 -> 29
# 43 -> 29

# 15 -> 39
# 39 -> 39

# 11 -> 35
# 35 -> 35

# 19 -> 19
# 43 -> 19