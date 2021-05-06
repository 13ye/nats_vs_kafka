#!/bin/bash

# 1. normal test 
./nats_vs_kafka work -m nats -t produce -c 2000000 &
./nats_vs_kafka work -m nats -t consume -c 2000000
sleep 1
echo "***********************************************"
echo "****************** KAFKA NOW ******************"
echo "***********************************************"
./nats_vs_kafka work -m kafka -t produce -c 2000000 &
./nats_vs_kafka work -m kafka -t consume -c 2000000

# 2. producer test
sleep 1
./nats_vs_kafka work -m nats -t produce -g 4 -c 1000000 &
./nats_vs_kafka work -m nats -t consume -g 4 -c 1000000
echo "***********************************************"
echo "****************** KAFKA NOW ******************"
echo "***********************************************"
./nats_vs_kafka work -m kafka -t produce -g 4 -c 1000000 &
./nats_vs_kafka work -m kafka -t consume -g 4 -c 1000000
