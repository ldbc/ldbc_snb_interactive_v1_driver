## Introduction

The driver is a load generator, designed for benchmarking database management systems.
It takes as input a workload definition, a database connector, and a set of configuration parameters. 
It then generates a workload (i.e., stream of operations) in conformance with the workload definition, and executes those operations against some system using the provided database connector.
During execution the driver continuously measures performance metrics, then upon completion it generates a report of those metrics.

It is capable of generating parallel workloads (e.g. concurrent reads and writes), while respecting the configured operation mix and ensuring that ordering between dependent operations is maintained (for more details see [Implementing workload operations](Implementing-Workload-Operations.md)).

The various components that make up the driver are: Configuration, Runtime, Workload, Database Connector, Metrics Collector, and Reporting. These are summarized below:

### Configuration
To provide a convenient means of customizing its behavior, the driver has many configuration parameters.
In addition, it has mechanisms that allow configuration parameters to be passed to implementations of its internal, pluggable components, e.g., `Workload` and `Db`.
For more information refer to [Driver configuration](Driver-Configuration.md), [Implementing a database connector](Implementing-a-Database-Connector.md), and [Implementing workload operations](Implementing-Workload-Operations.md).

### Runtime
The runtime, responsible for generating the actual load, is the most complex component.
Among other things it takes as input a workload definition (`Workload` instance) and a set of configuration parameters (`DriverConfiguration` instance).
It then configures the execution resources (e.g., thread pools) necessary to generate the desired workload,
and generates that load.

The complexity of the runtime is due, largely, to a number of properties that it ensures:
* Regardless of the hardware on which the driver runs, the generated workload will always be the same, i.e., the throughput of generated operations does not depend on the available resources.
If the environment in which the driver is run has insufficient resources, to generate the defined load, the driver will purposely terminate, providing an error message as to why it did so.
Rational for designing the driver to behave in this manner is that a workload definition is viewed (as much as possible) as the description of a scientific experiment, i.e., (as much as possible) it should be deterministic/repeatable.
* Via the _Workload_ and _Configuration_ components, the driver allows for constraints regarding the ordering between dependent operations to be defined. 
The runtime component must then be capable of executing that workload in parallel, while ensuring that those configured ordering constraints are maintained.

### Workload
When the driver runs it generates a workload: a stream of _operations_ (`Operation` instances) to be executed against some system.
The contents of that operation stream is defined by a workload definition, which can be created by extending the `Workload` class (for more details see [Implementing workload operations](Implementing-Workload-Operations.md)).

Every `Operation` in the generated stream has the following attributes: 
* **Type**: Used to group semantically equivalent operations. Example operation types would be _RetrieveUserProfile_, _RetrieveFriendsOfFriendsOfUser_, and _CreateNewUserProfile_.
* **Classification**: Defines the way an operation must be scheduled/executed. 
This is out of scope here, refer to [Implementing workload operations](Implementing-Workload-Operations.md) for more details.
* **Parameters**: Operations may be of the same semantic nature, yet still different.
For example, the _RetrieveUserProfile_ operation may have one parameter named _userId_, and two instances of _RetrieveUserProfile_ may have different values for _userId_, i.e., they refer to the profile of a different user.
* **Scheduled Start Time**: The wall clock time when an operation is due to be executed, i.e., when it is passed to the database connector. Explicitly assigning start times to operations is one of the techniques used by the driver that enable it to generate repeatable workloads.
* **Expected Result**: Every operation has an expected result, both in terms of format and in terms of content. This is used during validation.

Finally, a workload definition must result in a deterministic workload. That is, given the same workload definition the driver will generate the same stream of operations.

The driver ships with a number of workload definitions. At present it ships with one official LDBC workload (`LdbcSnbInteractiveWorkload`) and one example workload definition (`SimpleWorkload`) - more are in development. In addition, the driver includes a library of tools to assist and encourage others to define their own workloads (see [Implementing workload operations] for more on this).

### Database Connector
To execute the generated load against a real database management system the driver must be provided with a database connector.
The driver ships with a number of base classes, which must be extended and then passed to the driver before running a benchmark: `Db`, `OperationHandler`, and `DbConnectionState`.
These three classes are used to communicate with the system under test.

The driver must be provided with one `Db` implementation, one `OperationHandler` implementation per `Operation` type generated by the `Workload`, and (optionally) one `DbConnectionState` implementation.
See the [Implementing a database connector](Implementing-a-Database-Connector.md) for a tutorial and further explanation.

During execution the driver retrieves an `OperationHandler` instance, from the provided `Db`, for every `Operation` instance generated by the `Workload`, it then invokes the `OperationHandler.executeOperation(Operation)` method and records results and performance metrics.

The `Db` class acts as a proxy through which the driver retrieves `OperationHandler` instances.
It is responsible for registering `OperationHandler` classes with the driver during initialization, as well as for initializing and cleaning up the database connection before and after the benchmark run.

The `OperationHandler` class is as it sounds, it is not much more than a function that takes an `Operation` instance as input, and returns an `OperationResultReport`.

Finally, the `DbConnectionState` provides the mechanism for the driver to give `OperationHandler` instances access to managed shared state, e.g., connection pools. Implementing this class is optional, but is likely necessary.

### Metrics Collecting & Reporting
A number of components work together to measure performance data, collect the measurements, calculate metrics and, finally, generate and export those metrics as a report upon benchmark completion.
For more information regarding the metrics that are measured and they can be read from the generated report see [Reading benchmark results](Reading-Benchmark-Results.md).
