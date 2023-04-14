## Running a Benchmark

Once the necessary classes (`Workload`, `Db`, `OperationHandler`, `DbConnectionState`) have been implemented,
`Workload` and `Db` implementations have been validated, and configuration parameters have been selected, the benchmark can be run.

As explained in [[Driver configuration]], the driver can be configured both via command line arguments and from configuration files. 
Likely, as explained in [[Implementing a database connector]] and [[Implementing workload operations]], although all core driver parameters have their own predefined argument names, parameters passed to `Workload` and `Db` implementations do not. As such, when passing those in from commandline it is necessary to use the `-p name|value` notation.

The general patterns for configuring the driver from configuration files and commandline are detailed in [Driver configuration](Driver-Configuration.md) already, but for a concrete example using the `com.ldbc.driver.workloads.simple.SimpleWorkload` workload that is shipped with the driver see below:

``` console
java -cp target/jeeves-0.2-SNAPSHOT.jar com.ldbc.driver.Client \
    -db com.ldbc.driver.workloads.simple.db.BasicDb \
    -P workloads/simple/simpleworkload.properties \
    -P workloads/ldbc_driver_default.properties
```
