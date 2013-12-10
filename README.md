![LDBC Logo](ldbc_logo.png) 

# LDBC Driver

It is still at an early stage of development, but the LDBC Driver is being developed as part of the Linked Data Benchmark Council EU-funded research project and will be used to run the benchmark workloads developed and released by LDBC:
* http://www.ldbc.eu
* http://www.linkedin.com/groups/LDBC-4955240
* https://twitter.com/LDBCproject
* https://www.facebook.com/ldbcproject

### Try it

    git clone https://github.com/ldbc/ldbc_driver.git
    cd ldbc_driver
    ./build.sh

### Usage

	java -cp core-0.1-SNAPSHOT.jar com.ldbc.driver.Client -db <classname> -l | -t -oc <count> [-P
	       <file1:file2>] [-p <key=value>] -rc <count> [-s]  [-tc <count>] -w <classname>
	       
	   -db,--database <classname>       classname of the DB to use (e.g. com.ldbc.driver.db.basic.BasicDb)
	   -l,--load                        run the loading phase of the workload
	   -oc,--operationcount <count>     number of operations to execute (default: 0)
	   -P <file1:file2>                 load properties from file(s) - files will be loaded in the order provided
	   -p <key=value>                   properties to be passed to DB and Workload - these will override
	                                    properties loaded from files
	   -rc,--recordcount <count>        number of records to create during load phase (default: 0)
	   -s,--status                      show status during run
	   -t,--transaction                 run the transactions phase of the workload
	   -tc,--threadcount <count>        number of worker threads to execute with (default: CPU cores - 2)
	   -w,--workload <classname>        classname of the Workload to use (e.g.
	                                    com.ldbc.driver.workloads.simple.SimpleWorkload)

To test the driver try "simpleworkload", which has little configuration options and comprises of only basic key/value operations.
To run it:

    java -cp target/core-0.2-SNAPSHOT.jar com.ldbc.driver.Client -db com.ldbc.driver.workloads.simple.db.BasicDb -P workloads/simple/simpleworkload -load
    java -cp target/core-0.2-SNAPSHOT.jar com.ldbc.driver.Client -db com.ldbc.driver.workloads.simple.db.BasicDb -P workloads/simple/simpleworkload -transaction

### Credit where it's due
Though it has now diverged significantly, this project started as a fork of the YCSB framework:
* http://wiki.github.com/brianfrankcooper/YCSB/
* http://research.yahoo.com/Web_Information_Management/YCSB
