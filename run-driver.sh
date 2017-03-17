#!/bin/bash

operation_count=1000000
thread_count=1
time_compression_ratio=0.0001

# used to determine workload mode, if false just produces updates for kafka queue, if yes then runs on actual SUT
consume_mode=false

graph_name=sf3updates

# locator should point to remote-objects.yaml
locator=/hdd1/ldbc/utilities/apache-tinkerpop-gremlin-console/conf/remote-objects.yaml

# configuration params for benchmark run
conf_path=configuration/producer-ldbc.properties

# dataset location
dataset_location=/home/r32zhou/ldbc/dataset/datagen/sf3_updates
parameters_dir=$dataset_location/substitution_parameters
updates_dir=$dataset_location/social_network

# Database Implementation
db=ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDb

# jar file for the workload implementation
workload_impl=/home/r32zhou/ldbc/graph-benchmarking/snb-interactive-gremlin/target/snb-interactive-gremlin-1.0-SNAPSHOT-jar-with-dependencies.jar

# first argument is a boolean. Run debug mode if given true
if [ "$1" = true ] ; then
    JAVA="java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=1044"
else
    JAVA="java"
fi

exec $JAVA -cp "target/jeeves-0.3-SNAPSHOT.jar:src/main/resources:$workload_impl" com.ldbc.driver.Client -w com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload -oc $operation_count -P $conf_path -p "ldbc.snb.interactive.parameters_dir|$parameters_dir" -p "ldbc.snb.interactive.updates_dir|$updates_dir" -p "graphName|$graph_name" -p "locator|$locator" -db $db -tc $thread_count -tcr $time_compression_ratio -cu $consume_mode
