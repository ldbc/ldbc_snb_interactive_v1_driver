## Reading Benchmark Results

Upon benchmark completion the driver will output results to the console and, if it was configured to do so (e.g., with the `-rf` parameter), to a .json file.

Assume we run a benchmark from commandline as below:

``` console
java -cp jeeves.jar com.ldbc.driver.Client \
	-db com.mythical.BasicDb \
	-w com.workloads.imaginary.BasicWorkload \
	-oc 1000 \
	-rf basic_workload_results.json \
	-P workloads/ldbc_driver_default.properties
```

The output from this run would be written to console out, and additionally to _basic_workload_results.json_.
Let's now look at the metrics the driver actually measures, and the format in which it reports those metrics.

## Result Metrics

Two 'levels' of metrics are reported by the driver, _summary_ and _per-operation type_.
Summary metrics are measures that have been aggregated across an entire benchmark run, they are very simple.
Per operation type metrics drill deeper into how the system under test performed, and the results are grouped by operation type. See below for more explanations of each:

### Summary Metrics

* **Units**: Time units that latencies were measured in
* **Start Time**: Start time (wall clock time) of first operation in workload
* **Finish Time**: Time at which the last operation execution completed
* **Total Run Time**: Duration between _Start Time_ and _Finish Time_
* **Total Operation Count**: Number of operations that were executed during the benchmark run

### Per Operation Type Metrics

For every operation type three metric types are reported: _Run Time_, _Start Time Delay_, and _Result Code_.

**Run Time** is a report of the latency that each operation type took to execute. The statistics reported are: mean, minimum, maximum, and various percentiles.

**Execution Latency** is a measure of how close to scheduled start time the operations of this type were executed. See [[Introduction]] and [[Importance of adhering to the workload definition]] for more information.
This is not an indicator of database performance, but can be useful when configuring and provisioning the benchmarking environment.

**Result Code** refers to the integer result code component of the result returned by `OperationHandler` instances, i.e., the `int resultCode` from `Operation.buildResult(int resultCode, RESULT_TYPE result)` (see [[Implementing a database connector]]).
The reported result is a count of the number of times each `resultCode` was returned.
This is not an indicator of database performance, nor is `resultCode` used during database validation. It is simply there as a development/debugging aid, e.g., for returning: error codes, number of rows returned by given operation types, etc.

## Output Format

Results are exported as json format, as shown in the example results file below.

```json
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
```
