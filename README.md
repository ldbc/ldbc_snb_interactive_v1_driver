![LDBC Logo](ldbc_logo.png) 

# LDBC Driver

This driver is being developed as part of the Linked Data Benchmark Council EU-funded research project and will be used to run the benchmark workloads developed and released by LDBC:
* [LDBC Project Website](http://www.ldbc.eu)
* [LDBC Company Website](http://ldbcouncil.org)
* [LDBC LinkedIn Group](http://www.linkedin.com/groups/LDBC-4955240)
* [LDBC Twitter Account](https://twitter.com/LDBCproject)
* [LDBC Facebook Page](https://www.facebook.com/ldbcproject)

### Try it

    git clone https://github.com/ldbc/ldbc_driver.git
    cd ldbc_driver
    ./build.sh

### Usage

	java -cp core-VERSION.jar com.ldbc.driver.Client [-db <classname>] [-del <duration>] [-gctd <duration>]
	       [-oc <count>] [-P <file1:file2>] [-p <key=value>] [-pids <peerId1:peerId2>] [-rf <path>] [-s] [-tc
	       <count>] [-tcr <ratio>] [-tu <unit>] [-w <classname>]
	   -db,--database <classname>                    classname of the DB to use (e.g.
	                                                 com.ldbc.driver.workloads.simple.db.BasicDb)
	   -del,--toleratedexecutiondelay <duration>     duration (ms) an operation handler may miss its scheduled
	                                                 start time by
	   -gctd,--gctdeltaduration <duration>           safe duration (ms) between dependent operations
	   -oc,--operationcount <count>                  number of operations to execute (default: 0)
	   -P <file1:file2>                              load properties from file(s) - files will be loaded in the
	                                                 order provided
	                                                 first files are highest priority; later values will not
	                                                 override earlier values
	   -p <key=value>                                properties to be passed to DB and Workload - these will
	                                                 override properties loaded from files
	   -pids,--peeridentifiers <peerId1:peerId2>     identifiers/addresses of other driver workers (for
	                                                 distributed mode)
	   -rf,--resultfile <path>                       where benchmark results JSON file will be written (null =
	                                                 file will not be created)
	   -s,--status                                   show status during run
	   -tc,--threadcount <count>                     number of worker threads to execute with (default: 6)
	   -tcr,--timecompressionratio <ratio>           change duration between operations of workload
	   -tu,--timeunit <unit>                         time unit to use when gathering metrics.
	                                                 default:MILLISECONDS, valid:[NANOSECONDS, MICROSECONDS,
	                                                 MILLISECONDS, SECONDS, MINUTES]
	   -w,--workload <classname>                     classname of the Workload to use (e.g.
	                                                 com.ldbc.driver.workloads.simple.SimpleWorkload)


[home_1](wiki/home)

[[words|wiki]]

https://github.com/ldbc/ldbc_driver/wiki

### Result Output Format

	{
	  "unit": "MILLISECONDS",
	  "start_time": 1397561979770,
	  "finish_time": 1397561980670,
	  "total_duration": 900,
	  "total_count": 10,
	  "all_metrics": [
	    {
	      "name": "com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery1",
	      "count": 4,
	      "unit": "MILLISECONDS",
	      "run_time": {
	        "name": "Runtime",
	        "unit": "MILLISECONDS",
	        "count": 4,
	        "mean": 0,
	        "min": 0,
	        "max": 0,
	        "50th_percentile": 0,
	        "90th_percentile": 0,
	        "95th_percentile": 0,
	        "99th_percentile": 0
	      },
	      "start_time_delay": {
	        "name": "Start Time Delay",
	        "unit": "MILLISECONDS",
	        "count": 4,
	        "mean": 0,
	        "min": 0,
	        "max": 0,
	        "50th_percentile": 0,
	        "90th_percentile": 0,
	        "95th_percentile": 0,
	        "99th_percentile": 0
	      },
	      "result_code": {
	        "name": "Result Code",
	        "unit": "Result Code",
	        "count": 4,
	        "all_values": {
	          "0": 3,
	          "1": 1
	        }
	      }
	    },
	    {
	      "name": "com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery2",
	      "count": 6,
	      "unit": "MILLISECONDS",
	      "run_time": {
	        "name": "Runtime",
	        "unit": "MILLISECONDS",
	        "count": 6,
	        "mean": 0,
	        "min": 0,
	        "max": 0,
	        "50th_percentile": 0,
	        "90th_percentile": 0,
	        "95th_percentile": 0,
	        "99th_percentile": 0
	      },
	      "start_time_delay": {
	        "name": "Start Time Delay",
	        "unit": "MILLISECONDS",
	        "count": 6,
	        "mean": 0,
	        "min": 0,
	        "max": 0,
	        "50th_percentile": 0,
	        "90th_percentile": 0,
	        "95th_percentile": 0,
	        "99th_percentile": 0
	      },
	      "result_code": {
	        "name": "Result Code",
	        "unit": "Result Code",
	        "count": 6,
	        "all_values": {
	          "0": 4,
	          "1": 2
	        }
	      }
	    }
	  ]
	}

To test driver try "simpleworkload", which has few configuration options and comprises of only basic key/value operations:

	java -cp target/core-0.2-SNAPSHOT.jar com.ldbc.driver.Client -db com.ldbc.driver.workloads.simple.db.BasicDb -P workloads/simple/simpleworkload.properties -P src/main/resources/ldbc_driver_default.properties
