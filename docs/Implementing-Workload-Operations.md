## Implementing Workload Operations

### Principles

Assuming you have [implemented a database connection](Implementing-a-Database-Connector), you can now implement the workload operations by:

1. Providing operation handlers for all its operations. 
2. Registering these handlers within the `onInit` method of the connector you implemented. 

### Creating an operation handler
Operation handlers extend the `OperationHandler` class and should override its a single method `executeOperation`.
An operation handler performs three tasks:

1. Unpack operation parameters from the given operators. 
2. Construct and perform a query / operational plan to enact the required operation given the supplied parameters.
3. Reports the results of the operation (records for a read operation, status code for a write / update operation). 

Consider the following example extracted from a virtuoso reference implementation for the interactive workload's LdbcQuery1 operation:

```java
public static class LdbcQuery1ToVirtuoso implements OperationHandler<LdbcQuery1, VirtuosoDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery1 operation, VirtuosoDbConnectionState state, ResultReporter resultReporter) throws DbException {
        	Connection conn = state.getConn();
		List<LdbcQuery1Result> RESULT = new ArrayList<LdbcQuery1Result>();
        	int results_count = 0; RESULT.clear();
        	try {
        		String queryString = file2string(new File(state.getQueryDir(), "query1.txt"));
        		if (state.isRunSql()) {
        			queryString = queryString.replaceAll("@Person@", String.valueOf(operation.personId()));
        			queryString = queryString.replaceAll("@Name@", operation.firstName());
        		}
                ...
                redacted
                ...
                Statement stmt = conn.createStatement();
                ResultSet result = stmt.executeQuery(queryString);
                ...
                redacted
                ...
        	resultReporter.report(results_count, RESULT, operation);
        }
    }
```

A prepared query statement is modified to include the given personId and friend first name. It is then executed and the results are collected in an arraylist which is supplied to the result reporter. 

For update operations with no expected result, you can used the supplied static LdbcNoResult class as follows:

```java
    resultReporter.report(0, LdbcNoResult.INSTANCE,operation);
```

## Reference Implementations

Visit the https://github.com/ldbc/ldbc_snb_interactive_impls/tree/v1-dev branch for reference implementations.
