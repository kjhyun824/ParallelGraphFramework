#!/bin/bash

num_core=$1
num_thread=$2

cpu_max_total_thread=32
cpu_number_of_core=16
max_i=31

for i in {1..31}
do
    if [ ${num_core} -gt ${i} ]
    then 
        echo 1 > /sys/devices/system/cpu/cpu${i}/online
    elif [ ${num_core} -le ${i} ]
    then 
        if [ ${i} -lt ${cpu_number_of_core} ]
        then 
            echo 0 > /sys/devices/system/cpu/cpu${i}/online
        elif [ ${num_core} -eq ${num_thread} ];
        then 
            echo 0 > /sys/devices/system/cpu/cpu${i}/online
        elif [ $((${num_core} * 2)) -eq ${num_thread} ];
        then
            if [ ${i} -lt $((${num_core} + ${cpu_number_of_core})) ];
            then 
                echo 1 > /sys/devices/system/cpu/cpu${i}/online
            else
                echo 0 > /sys/devices/system/cpu/cpu${i}/online
            fi 
        fi
    fi
done
            
        
