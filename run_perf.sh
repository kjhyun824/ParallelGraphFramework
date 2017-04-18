#!/bin/bash

DATA="LiveJournal"
#DATA="FriendSter"
#DATA="TwitterMPI"
#DATA="TwitterWWW"

File="/home/woori4829/data/LiveJournal_weighted.txt"
#File="/home/woori4829/data/FriendSter_weighted.txt"
#File="/home/woori4829/data/TwitterMPI_weighted.txt"
#File="/home/woori4829/data/TwitterWWW_weighted.txt"

NUM_THREADS="4"
DELTAS="0.25"

for thr in $NUM_THREADS; do
	for delta in $DELTAS; do
		perf stat java -server -Xms16g -Xmx16g -da -XX:BiasedLockingStartupDelay=0 -XX:MaxInlineSize=256 -XX:FreqInlineSize=256 -jar /home/woori4829/Dropbox/JungBong/build/jar/SSSP.jar ${File} ${thr} ${delta}
	done
done
