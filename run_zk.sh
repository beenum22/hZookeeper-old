#!/usr/bin/env bash
set -x
set -e
WorkDir=`pwd`
znode_creation_count=$1
client_count=$2
znode_data=$3
znode_modification_count=$4
stress_reader=$5

zk_server_ip=$6

${WorkDir}/src/zk_main.py ${znode_creation_count} ${client_count} ${znode_data} ${znode_modification_count} ${stress_reader} ${zk_server_ip}

#${WorkDir}/src/zk_main.py ${zk_server_ip}
