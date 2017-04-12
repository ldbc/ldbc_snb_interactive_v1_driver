#!/bin/bash

# conf_path=configuration/ldbc_snb_interactive_SF-0001_titan.properties
conf_path=/home/apacaci/Projects/jimmy/ldbc_snb_interactive_validation/sparksee/readwrite_sparksee--ldbc_driver_config--db_validation.properties
parameters_dir=/home/apacaci/Projects/jimmy/ldbc_snb_interactive_validation/sparksee/validation_set


# Database Implementation
db=ca.uwaterloo.cs.ldbc.interactive.sql.PostgresDb

# jar file for the workload implementation
# jar file for the workload implementation
workload_impl=lib/snb-interactive-sql-1.0-SNAPSHOT-jar-with-dependencies.jar
postgres_conf=lib/postgres_configuration.properties

# if first argument is true, then it will run in debug mode
if [ "$1" = true ] ; then
    JAVA="java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=1044"
else
    JAVA="java"
fi

exec $JAVA -cp "target/jeeves-0.3-SNAPSHOT.jar:$workload_impl" com.ldbc.driver.Client -w com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload -P $conf_path -p "ldbc.snb.interactive.parameters_dir|$parameters_dir" -db $db -p "ldbc.snb.interactive.update_interleave|1" -vdb /home/apacaci/Projects/jimmy/ldbc_snb_interactive_validation/sparksee/validation_set/validation_params.csv -P $postgres_conf
