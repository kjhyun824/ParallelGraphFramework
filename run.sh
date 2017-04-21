#!/bin/bash

Pink='\033[0;31m'
Green='\033[0;32m'
NC='\033[0m'
INFO="[${Pink}INFO${NC}]"
_DATA="[${Pink}DATA${NC}]"
ARGS="[${Pink}ARGS${NC}]"
DONE="[${Green}DONE${NC}]"

#DATA="LiveJournal"
#DATA="FriendSter"
#DATA="TwitterMPI"
DATA="TwitterWWW"

#File="/home/woori4829/data/LiveJournal_noself_weighted.txt"
#File="/home/woori4829/data/FriendSter_weighted.txt"
#File="/home/woori4829/data/TwitterMPI_weighted.txt"
File="/home/woori4829/data/TwitterWWW_weighted.txt"

#NUM_THREADS="4 8 16"
NUM_THREADS="16"
DELTAS="20 50 80"
#NUM_THREADS="8"
#DELTAS="0.5"

for thr in $NUM_THREADS; do
	mkdir -p /home/woori4829/Dropbox/SSSP_Output/${DATA}/Thread${thr}/Sync
	for delta in $DELTAS; do
		echo -e "${INFO} SSSP Running"
		echo -e "${_DATA} $DATA"
		echo -e "${ARGS} Thread : ${Pink}${thr}${NC}"
		echo -e "${ARGS} Delta : ${Pink}${delta}${NC}"
		echo -e "${ARGS} Sync : ${Pink}True${NC}"
		java -server -Xms16g -Xmx16g -da -XX:BiasedLockingStartupDelay=0 -XX:MaxInlineSize=256 -XX:FreqInlineSize=256 -jar /home/woori4829/Dropbox/JungBong/build/jar/SSSP.jar ${File} ${thr} ${delta} 0 > /home/woori4829/Dropbox/SSSP_Output/${DATA}/Thread${thr}/Sync/SSSP_thr${thr}_delta${delta}_sync.txt
		echo -e "${DONE}\n"
	done
done

for thr in $NUM_THREADS; do
	mkdir -p /home/woori4829/Dropbox/SSSP_Output/${DATA}/Thread${thr}/Async
	for delta in $DELTAS; do
		echo -e "${INFO} SSSP Running"
		echo -e "${_DATA} $DATA"
		echo -e "${ARGS} Thread : ${Pink}${thr}${NC}"
		echo -e "${ARGS} Delta : ${Pink}${delta}${NC}"
		echo -e "${ARGS} Sync : ${Pink}False${NC}"
		java -server -Xms16g -Xmx16g -da -XX:BiasedLockingStartupDelay=0 -XX:MaxInlineSize=256 -XX:FreqInlineSize=256 -jar /home/woori4829/Dropbox/JungBong/build/jar/SSSP.jar ${File} ${thr} ${delta} 1 > /home/woori4829/Dropbox/SSSP_Output/${DATA}/Thread${thr}/Async/SSSP_thr${thr}_delta${delta}_async.txt
		echo -e "${DONE}\n"
	done
done
