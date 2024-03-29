#!/usr/bin/env bash
set -x
set -e
WorkDir=`pwd`
#znode_creation_count=$1
#client_count=$2
#znode_data=$3
#znode_modification_count=$4
#stress_reader=$5

zk_server_ip=$1
test_type=$2
stress_type=$3
stress_clients=$4
threads_per_client=$5

${WorkDir}/src/case_1/zk_main.py ${zk_server_ip} ${test_type} ${stress_type} ${stress_clients} ${threads_pre_client}

