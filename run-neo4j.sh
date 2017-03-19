#!/bin/bash

operation_count=100
thread_count=1
time_compression_ratio=0.0001

# used to determine workload mode, if false just produces updates for kafka queue, if yes then runs on actual SUT
consume_mode=true

# configuration params for benchmark run
conf_pathsq1=/home/r32zhou/ldbc/ldbc_driver/configuration/consumer-ldbc-sq1.properties
conf_pathsq3=/home/r32zhou/ldbc/ldbc_driver/configuration/consumer-ldbc-sq3.properties
conf_pathq11=/home/r32zhou/ldbc/ldbc_driver/configuration/consumer-ldbc-q11.properties
conf_pathq13=/home/r32zhou/ldbc/ldbc_driver/configuration/consumer-ldbc-q13.properties
conf_path=$conf_pathq13

# dataset location
dataset_location_sf3=/home/r32zhou/ldbc/dataset/datagen/sf3_updates
dataset_location_sf10=/home/r32zhou/ldbc/dataset/datagen/sf10_updates

dataset_location=$dataset_location_sf10

parameters_dir=$dataset_location/substitution_parameters
updates_dir=$dataset_location/social_network

# Database Implementation
db=net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb
neo4jhost=localhost
neo4jport=7474

# jar file for the workload implementation
workload_impl=lib/snb-interactive-neo4j-1.0.0-jar-with-dependencies.jar

# first argument is a boolean. Run debug mode if given true
if [ "$1" = true ] ; then
    JAVA="java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=1044"
else
    JAVA="java"
fi

exec $JAVA -cp "target/jeeves-0.3-SNAPSHOT.jar:src/main/resources:$workload_impl" com.ldbc.driver.Client -w com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload -oc $operation_count -P $conf_path -p "ldbc.snb.interactive.parameters_dir|$parameters_dir" -p "ldbc.snb.interactive.updates_dir|$updates_dir" -p "host|$neo4jhost" -p "port|$neo4jport" -db $db -tc $thread_count -tcr $time_compression_ratio -cu $consume_mode
