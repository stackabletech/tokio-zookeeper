#!/bin/bash

set -x

mkdir /tmp/data
echo -e "tickTime=2000\ndataDir=/tmp/data\nclientPort=2181" > zookeeper/conf/zoo.cfg
./zookeeper/bin/zkServer.sh start
