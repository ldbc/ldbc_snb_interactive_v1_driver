#!/bin/bash

operation_count=10000
thread_count=64
time_compression_ratio=0.0001

# used to determine workload mode, if false just produces updates for kafka queue, if yes then runs on actual SUT
consume_mode=true

# configuration params for benchmark run
conf_pathsq1=/home/apacaci/Projects/jimmy/ldbc_driver/configuration/consumer-ldbc-sq1.properties
conf_pathsq3=/home/apacaci/Projects/jimmy/ldbc_driver/configuration/consumer-ldbc-sq3.properties
conf_pathq11=/home/apacaci/Projects/jimmy/ldbc_driver/configuration/consumer-ldbc-q11.properties
conf_pathq13=/home/apacaci/Projects/jimmy/ldbc_driver/configuration/consumer-ldbc-q13.properties
conf_path_short=/home/apacaci/Projects/jimmy/ldbc_driver/configuration/consumer-ldbc-short.properties
conf_path=$conf_path_short

# dataset location
dataset_location_sf3=/home/apacaci/Projects/jimmy/ldbc_snb_dategen/datasets/sf3_updates
dataset_location_sf10=/home/apacaci/Projects/jimmy/ldbc_snb_datagen/datasets/sf10_updates
dataset_location_validation=/home/apacaci/Projects/jimmy/ldbc_snb_datagen/datasets/sparksee_validation_set

dataset_location=$dataset_location_validation

parameters_dir=$dataset_location/substitution_parameters
updates_dir=$dataset_location/social_network

# Database Implementation
db=ca.uwaterloo.cs.ldbc.interactive.sql.PostgresDb

# jar file for the workload implementation
workload_impl=lib/snb-interactive-sql-1.0-SNAPSHOT-jar-with-dependencies.jar
postgres_conf=lib/postgres_configuration.properties

# first argument is a boolean. Run debug mode if given true
if [ "$1" = true ] ; then
    JAVA="java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=1044"
else
    JAVA="java"
fi

exec $JAVA -cp "target/jeeves-0.3-SNAPSHOT.jar:src/main/resources:$workload_impl:$virtjdbc4" com.ldbc.driver.Client -w com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload -oc $operation_count -P $conf_path -p "ldbc.snb.interactive.parameters_dir|$parameters_dir" -p "ldbc.snb.interactive.updates_dir|$updates_dir" -db $db -tc $thread_count -tcr $time_compression_ratio -cu $consume_mode -P $postgres_conf
