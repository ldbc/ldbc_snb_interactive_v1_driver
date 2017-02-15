#!/bin/bash

operation_count=1000
thread_count=1
time_compression_ratio=0.0001

# used to determine workload mode, if false just produces updates for kafka queue, if yes then runs on actual SUT
consume_mode=true

graph_name=sf3titan11RW

# cassandra / hbase / gremlin / gremlinkafka
backend=gremlin
# backend=cassandra

# Titan Specific Parametersif backend is gremlin, then locator should point to remote-objects.yaml
locator=/home/apacaci/Projects/jimmy/titan11/conf/remote-objects.yaml
# locator=tem22

# Kafka Specific Conf parameters
producer=configuration/producer.properties

#Neo4j Specific Parameters
host=localhost
port=7474

# configuration params for benchmark run
conf_path=configuration/consumer-ldbc_snb_interactive_SF-0001_titan.properties

# dataset location
dataset_location=/u4/apacaci/Projects/jimmy/ldbc_snb_datagen/datasets/sf1_updates
parameters_dir=$dataset_location/substitution_parameters
updates_dir=$dataset_location/social_network

# validation run parameters
# conf_path=/u4/apacaci/Projects/jimmy/ldbc_snb_interactive_validation/sparksee/readwrite_sparksee--ldbc_driver_config--db_validation.properties
# parameters_dir=/u4/apacaci/Projects/jimmy/ldbc_snb_interactive_validation/sparksee/validation_set
# updates_dir=/u4/apacaci/Projects/jimmy/ldbc_snb_interactive_validation/sparksee/validation_set

# Database Implementation
# db=net.ellitron.ldbcsnbimpls.interactive.titan.TitanDb
# db=net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb
db=ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDb

# jar file for the workload implementation
workload_impl=/home/apacaci/Projects/jimmy/codebase/snb-interactive-gremlin/target/snb-interactive-gremlin-1.0-SNAPSHOT-jar-with-dependencies.jar
# workload_impl=/home/apacaci/ldbc-gremlin/ldbc-snb-impls/snb-interactive-neo4j/target/snb-interactive-neo4j-1.0.0-jar-with-dependencies.jar
# workload_impl=/home/apacaci/ldbc-gremlin/ldbc-snb-impls/snb-interactive-titan/target/snb-interactive-titan-0.1.0-jar-with-dependencies.jar
# workload_impl=/home/apacaci/ldbc-gremlin/ldbc

# first argument is a boolean. Run debug mode if given true
if [ "$1" = true ] ; then
    JAVA="java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=1044"
else
    JAVA="java"
fi

exec $JAVA -cp "target/jeeves-0.3-SNAPSHOT.jar:src/main/resources:$workload_impl" com.ldbc.driver.Client -w com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload -oc $operation_count -P $conf_path -p "ldbc.snb.interactive.parameters_dir|$parameters_dir" -p "ldbc.snb.interactive.updates_dir|$updates_dir" -p "graphName|$graph_name" -p "locator|$locator" -p "backend|$backend" -db $db -p "kafka_config|$producer" -p "host|$host" -p "port|$port" -tc $thread_count -tcr $time_compression_ratio -cu $consume_mode
fi
