## Validating a Database Connector

After a database connector (i.e., implementations of `Db`, `OperationHandler`, `DbConnectionState`) has been created, it needs to be tested for correctness. In addition to whatever tests you have (hopefully) performed yourself, the driver provides a mechanism for testing your database connector against a given workload and dataset.

It is the responsibility of the workload developer to also provide a _validation dataset_ and _validation operation set_, which others can then use to test their database connector implementations.

Logically, the _validation operation set_ is a stream of _operation-result_ pairs, and in practice it could be realized with a _pipe-separated file_ (e.g. `validation_parameters.csv`):

```
serializedOperation|serializedOperationResult
```

The general process is as follows:
1. Implement database connector
2. Load validation dataset into database under test
3. Run validation operation set against database, using database connector

This is explained in a little more detail below, using the example database and workload that were introduced in [Implementing a database connector](Implementing-a-Database-Connector.md).

### Imaginary Example

You'll recall, our example workload contained two operations: `ReadOperation` and `UpdateOperation`.
As only `ReadOperation` returns a value, this is the only operation type that will appear in the validation operation set for this workload.

Remember also that `ReadOperation` has three parameters (`String table`, `String key`, and `List<String> fields`) and returned an `Integer`. 
Given this information, the validation operation set file could have the following format:

```
[operationType,table,key,fields]|result
```

And the following content:

```
["com.workloads.imaginary.ReadOperation","tableX","key1",["fieldA","fieldB"]]|1
["com.workloads.imaginary.ReadOperation","tableY","key2",["fieldC","fieldD"]]|2
["com.workloads.imaginary.ReadOperation","tableZ","key3",["fieldE","fieldF"]]|42
...
...
```

Finally, to validate our database connector (`BasicDb`) we run the driver with the `-vdb/--validatedatabase` parameter (see [Driver configuration](Driver-Configuration.md) for more details), as follows:

``` console
java -cp target/core-0.2-SNAPSHOT.jar com.ldbc.driver.Client 
-db com.mythical.BasicDb
-w com.workloads.imaginary.BasicWorkload
-vdb workloads/imaginary/validation_parameters.csv
-P workloads/imaginary/imaginary_workload.properties 
-P workloads/ldbc_driver_default.properties
```

The driver will then execute each operation in the validation operation set, and compare the results from `com.mythical.BasicDb` against the expected results, obtained from `workloads/imaginary/validation_parameters.csv`.

If the database connector returns correct results for every operation, the driver will output something like:

``` console
Database Validation Successful
```

If the database connector returns an correct result, the driver will output something like:

``` console
Database validation failed
Invalid operation result
Operation: ReadOperation{table="tableX", key="key1", fields=["fieldA","fieldB"]}
Expected Result: 1
Actual Result: 2
```

Note that the validation parameters override any specific query enablement and operation count parameters set in other property files. See [this issue](https://github.com/ldbc/ldbc_driver/issues/50) for details. 

Also, see [SNB interactive validation repository](https://github.com/ldbc/ldbc_snb_interactive_validation) for a real validation set.
