#!/bin/bash

DATA="LiveJournal"
#DATA="FriendSter"
#DATA="TwitterMPI"
#DATA="TwitterWWW"

File="/home/woori4829/data/LiveJournal_noself_weighted.txt"
#File="/home/woori4829/data/FriendSter_weighted.txt"
#File="/home/woori4829/data/TwitterMPI_weighted.txt"
#File="/home/woori4829/data/TwitterWWW_weighted.txt"

#NUM_THREADS="1 2 4 8 16"
#DELTAS="0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 1.0"
NUM_THREADS="8"
DELTAS="0.5"

for thr in $NUM_THREADS; do
	for delta in $DELTAS; do
		java -server -Xms16g -Xmx16g -da -XX:BiasedLockingStartupDelay=0 -XX:MaxInlineSize=256 -XX:FreqInlineSize=256 -jar /home/woori4829/Dropbox/JungBong/build/jar/SSSP.jar ${File} ${thr} ${delta} > /home/woori4829/Dropbox/SSSP_Output/${DATA}/SSSP_thr${thr}_delta${delta}.txt
	done
done
