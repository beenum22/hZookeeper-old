#!/usr/bin/env bash
set -x
set -e
WorkDir=`pwd`
num_msgs=$1
client_count=$2
#threads=$3
#zk_server_ip=$3
${WorkDir}/src/zk_test.py ${num_msgs} ${client_count} 
