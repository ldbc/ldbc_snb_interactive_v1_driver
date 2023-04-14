## Driver configuration

There are two groups of configuration parameters:
* **Core parameters**: 
These parameters have special meaning for the driver itself, they have reserved names and explained individually below.
* **User-defined parameters**: 
These parameters have no special meaning to, nor are they recognized by, the driver.
Their names and expected values/formats are defined by the authors of `Workload` and `Db` implementations.
During driver initialization they are passed to `Workload` implementations via `Workload.onInit(Map<String, String> params)` (for more details refer to [Implementing workload operations](Implementing-Workload-Operations.md) and to `Db` implementations via `Db.onInit(Map<String, String> params)` (for more details refer to [Implementing a database connector](Implementing-a-Database-Connector.md)).

### Parameter meanings

First of all, some parameters control complex internal mechanisms within the driver, and therefore should not be changed without a solid understanding of why these mechanisms exist and how they operate. 
Although all parameters will be listed below, a discussion of the more complex settings is out of scope here. 

#### Common

* `status`: **integer**. Interval (in seconds) between each time status is printed. If `0`, status printouts will be disabled
* `thread_count`: **integer**. Size of thread pool to use for execution `OperationHandler` instances
* `results_dir`: **string**. Path to where the benchmark results will be written
* `time_unit`: **enum**. The time unit performance metrics will be measured and reported in. Possible values are: NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS, MINUTES
* `validate_database`: **string**. Path that specifies where to find the validation parameters file (see [Validating a database connector](Validating-a-Database-Connector.md)).
If parameter is set database validation is enabled, i.e., driver will check if the provided database implementation is correct: a series of tests will be executed against the provided `Db` implementation. Useful when developing a database connector.
* `workload_statistics`: **boolean**. If `true` the driver will calculate things like operation mix, operation count, target throughput, the time intervals between `Operation` instances of different types, and more. It will then print those statistics to the console. Generally useful for gaining a better understanding of the behavior of a given `Workload` implementation, and especially useful when developing your own `Workload` implementation.
* `operation_count`: **long**. Specifies the number of operations to generate during benchmark execution
* `workload`: **string**. Specifies the `Workload` implementation to use. The value of this parameter should be the fully qualified class name of the `Workload` subclass, e.g., `com.workloads.imaginary.BasicWorkload`
* `database`: **string**. Specifies the `Db` implementation to use. Fully qualified class name of the 'Db' subclass, e.g., `com.mythical.BasicDb`
* `spinner_wait_duration`: **long** (milliseconds). The driver is designed in such a way that it blocks as little as possible. For example, when waiting for the scheduled start time of the next operation to execute it will repeatedly poll the current time, rather than sleeping. A side-effect of this approach is high CPU load, which can be an issue if (for some reason) you choose to run the driver on the same machine as the database under test. To address this issue (i.e., reduce CPU consumption), `spinnerwaitduration` allows for a sleep (`Thread.sleep(milliseconds)`) duration to be injected into the busy-wait loops. Note, if `spinnerwaitduration=0` no sleep will be injected.

#### Advanced

* `time_compression_ratio`: **double**. As covered in various sections (including [Introduction](Introduction.md)), the driver executes a stream of _operations_, and schedules those executions according to the scheduled start times of those operations. The generated load is not a function of the environment used to run the driver, nor is it controlled by the system under test. Therefore, to generate a more or less demanding workload from the same workload definition (same operation mix, same operation parameters, same ordering, etc.) the driver provides a mechanism for _compressing_/_stretching_ an operation stream such that the intervals between operations is increased or decreased, proportionately for the entire stream.
For example, a value of 2.0 means the benchmark will run 2x slower/longer, 0.1 will run 10x faster/shorter, and 1.0 (default) will leave the benchmark unchanged.
* `create_validation_parameters`: **(string,integer)**. Controls the generation of validation parameters for validating the correctness of database connector implementations. The parameter value is a 2-tuple, where the first entry specifies where to create the validation parameters file and the second specifies how many validation parameters to generate, e.g., `workloads/imaginary/basicworkload/validation_parameters.csv|1000`

### Passing parameters to the driver

Regardless of the meaning of a configuration parameter, all parameters are passed to the driver in one of two ways: 

* **Configuration files**: 
    Configuration parameters can be passed to the driver by setting them in `.properties` files and then specifying (via commandline, using `-P file1|file2|...`) where those files can be found.
    Note, configuration files will be applied in the order they are entered. 
    That is, if multiple configuration files contain values for the same parameters, parameter values from configuration files appearing earlier in the command line will have precedence over (overwrite) parameter values from configuration files appearing later.
    See also [Running a benchmark](Running-a-Benchmark.md).
* **Command line**: 
    Configuration parameters can be passed to the driver via command line arguments.
    Every _core_ parameter has a predefined name and is set using a specific notation, see below.
    To pass in _user-defined_ parameters use the `-p name|value` notation.
    Note, any parameter values set via command line will have precedence over (overwrite) values set via configuration file.

#### Setting Parameters via Command Line

Use the `--help` parameter for instructions regarding how to set driver parameters from the command line. 
