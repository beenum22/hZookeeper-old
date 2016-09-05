#!/usr/bin/env bash
set -x
set -e
WorkDir=`pwd`
znode_creation_count=$1
client_count=$2
znode_data=$3
znode_deletion_count=$4
#zk_server_ip=$5

${WorkDir}/src/zk_test.py ${znode_creation_count} ${client_count} ${znode_data} ${znode_deletion_count} 
