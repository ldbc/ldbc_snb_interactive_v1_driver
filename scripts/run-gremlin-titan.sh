#!/bin/bash

operation_count=100
thread_count=16
time_compression_ratio=0.0001

# used to determine workload mode, if false just produces updates for kafka queue, if yes then runs on actual SUT
consume_mode=true

graph_name=sf10_updates

# locator should point to remote-objects.yaml
locatorgremlin=/hdd1/ldbc/utilities/apache-tinkerpop-gremlin-console-3.2.3/conf/remote-objects.yaml
locatortitan=/hdd1/ldbc/utilities/titan11/conf/remote-objects.yaml

locator=$locatortitan

# configuration params for benchmark run
conf_pathsq1=/home/r32zhou/ldbc/ldbc_driver/configuration/consumer-ldbc-sq1.properties
conf_pathsq3=/home/r32zhou/ldbc/ldbc_driver/configuration/consumer-ldbc-sq3.properties
conf_pathq11=/home/r32zhou/ldbc/ldbc_driver/configuration/consumer-ldbc-q11.properties
conf_pathq13=/home/r32zhou/ldbc/ldbc_driver/configuration/consumer-ldbc-q13.properties
conf_path=$conf_pathsq3

# dataset location
dataset_location_sf3=/home/r32zhou/ldbc/dataset/datagen/sf3_updates
dataset_location_sf10=/home/r32zhou/ldbc/dataset/datagen/sf10_updates

dataset_location=$dataset_location_sf10

parameters_dir=$dataset_location/substitution_parameters
updates_dir=$dataset_location/social_network

# Database Implementation
db=ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDb

# jar file for the workload implementation
workload_impl_gremlin=/home/r32zhou/ldbc/graph-benchmarking/snb-interactive-gremlin/target/snb-interactive-gremlin-1.0-SNAPSHOT-jar-with-dependencies.jar
workload_impl_titan=/home/r32zhou/ldbc/graph-benchmarking/snb-interactive-gremlin/target/snb-interactive-gremlin-1.0-titan-SNAPSHOT-jar-with-dependencies.jar
workload_impl=$workload_impl_titan

# first argument is a boolean. Run debug mode if given true
if [ "$1" = true ] ; then
    JAVA="java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=1044"
else
    JAVA="java"
fi

exec $JAVA -cp "target/jeeves-0.3-SNAPSHOT.jar:src/main/resources:$workload_impl" com.ldbc.driver.Client -w com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload -oc $operation_count -P $conf_path -p "ldbc.snb.interactive.parameters_dir|$parameters_dir" -p "ldbc.snb.interactive.updates_dir|$updates_dir" -p "graphName|$graph_name" -p "locator|$locator" -db $db -tc $thread_count -tcr $time_compression_ratio -cu $consume_mode
