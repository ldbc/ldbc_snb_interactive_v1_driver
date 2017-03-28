#!/bin/bash

operation_count=1000
thread_count=1
time_compression_ratio=0.0001

# used to determine workload mode, if false just produces updates for kafka queue, if yes then runs on actual SUT
consume_mode=true

# configuration params for benchmark run
conf_pathsq1=/home/r32zhou/ldbc/ldbc_driver/configuration/consumer-ldbc-sq1.properties
conf_pathsq3=/home/r32zhou/ldbc/ldbc_driver/configuration/consumer-ldbc-sq3.properties
conf_pathq11=/home/r32zhou/ldbc/ldbc_driver/configuration/consumer-ldbc-q11.properties
conf_pathq13=/home/r32zhou/ldbc/ldbc_driver/configuration/consumer-ldbc-q13.properties
conf_path=$conf_pathsq3

# dataset location
dataset_location_sf3=/home/r32zhou/ldbc/dataset/datagen/sf3_updates
dataset_location_sf10=/home/r32zhou/ldbc/dataset/datagen/sf10_updates

dataset_location=$dataset_location_sf3

parameters_dir=$dataset_location/substitution_parameters
updates_dir=$dataset_location/social_network

# Database Implementation
db=com.ldbc.driver.workloads.ldbc.snb.interactive.db.VirtuosoDb

# jar file for the workload implementation
workload_impl=lib/virtuoso_int-0.0.1-SNAPSHOT.jar
virtjdbc4=lib/virtjdbc4.jar
virtuoso_conf=lib/virtuoso_configuration.properties

# first argument is a boolean. Run debug mode if given true
if [ "$1" = true ] ; then
    JAVA="java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=1044"
else
    JAVA="java"
fi

exec $JAVA -cp "target/jeeves-0.3-SNAPSHOT.jar:src/main/resources:$workload_impl:$virtjdbc4" com.ldbc.driver.Client -w com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload -oc $operation_count -P $conf_path -p "ldbc.snb.interactive.parameters_dir|$parameters_dir" -p "ldbc.snb.interactive.updates_dir|$updates_dir" -db $db -tc $thread_count -tcr $time_compression_ratio -cu $consume_mode -P $virtuoso_conf
