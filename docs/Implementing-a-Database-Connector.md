## Implementing a Database Connector

Imagine we are implementing a database connector for some fictional workload. 
We will begin with a brief summary of the workload, then move on to how a database connector would be implemented for that workload.

#### Workload
Consider a fictional workload that generates an operation stream consisting of only two different operation types: `ReadOperation` and `UpdateOperation`.

```java
public class ReadOperation extends Operation<Integer> {
    private final String table;
    private final String key;
    private final List<String> fields;
    public ReadOperation(String table, String key, List<String> fields) {
        this.table = table;
        this.key = key;
        this.fields = fields;
    }
    public String getTable() {
        return table;
    }
    public String getKey() {
        return key;
    }
    public List<String> getFields() {
        return fields;
    }
}
```
```java
public class UpdateOperation extends Operation<Object> {
    private final String table;
    private final String key;
    private final Map<String, ByteIterator> values;
    public UpdateOperation(String table, String key, Map<String, ByteIterator> values) {
        this.table = table;
        this.key = key;
        this.values = values;
    }
    public String getTable() {
        return table;
    }
    public String getKey() {
        return key;
    }
    public Map<String, ByteIterator> getValues() {
        return values;
    }
}
```
These class definitions tell us two things about these operations: (1) their parameters (2) their expected result types. The generic type parameter tells us the expected result type, and the getter methods tell us what parameters the operations contain.

For example, from `ReadOperation extends Operation<Integer>` we know the expected result type is `Integer`, and from the getters we know the parameters are `String table`, `String key`, and `List<String> fields`.

#### Database Connector
To implement a database connector for this workload we need to extend three classes: `Db`, `OperationHandler`, and `DbConnectionState`.
We'll explain each one shortly, but first please refer to the complete code for this fictional database connector, below.

```java
public class BasicDb extends Db {
    static class BasicClient {
        BasicClient(String connectionUrl) {
            // TODO add actual initialization
        }
        Object execute(String queryString, Map<String, Object> queryParams) {
            // TODO add actual logic
            return null;
        }
    }

    static class BasicDbConnectionState extends DbConnectionState {
        private final BasicClient basicClient;
        private BasicDbConnectionState(String connectionUrl) {
            basicClient = new BasicClient(connectionUrl);
        }
        BasicClient client() {
            return basicClient;
        }
    }

    private BasicDbConnectionState connectionState = null;

    @Override
    public void onInit(Map<String, String> properties) throws DbException {
        String connectionUrl = properties.get("url");
        connectionState = new BasicDbConnectionState(connectionUrl);
        registerOperationHandler(ReadOperation.class, ReadOperationHandler.class);
        registerOperationHandler(UpdateOperation.class, UpdateOperationHandler.class);
    }

    @Override
    public void onCleanup() throws DbException {
        // perform any necessary cleaning up
    }

    public static class ReadOperationHandler extends OperationHandler<ReadOperation> {
        @Override
        public OperationResultReport executeOperation(ReadOperation operation) {
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("table", operation.getTable());
            queryParams.put("key", operation.getKey());
            queryParams.put("fields", operation.getFields());

            // TODO replace with actual query string
            String queryString = null;

            BasicClient client = ((BasicDbConnectionState) dbConnectionState()).client();
            Integer result = (Integer) client.execute(queryString, queryParams);

            return operation.buildResult(0, result);
        }
    }

    public static class UpdateOperationHandler extends OperationHandler<UpdateOperation> {
        @Override
        public OperationResultReport executeOperation(UpdateOperation operation) {
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("table", operation.getTable());
            queryParams.put("key", operation.getKey());
            queryParams.put("values", operation.getValues());

            // TODO replace with actual query string
            String queryString = null;

            BasicClient client = ((BasicDbConnectionState) dbConnectionState()).client();
            Object result = client.execute(queryString, queryParams);

            return operation.buildResult(0, result);
        }
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return connectionState;
    }
}
```
##### OperationHandler
Starting with the `OperationHandler` implementations, we see there there two implementations (`ReadOperationHandler` and `UpdateOperationHandler`) one for each `Operation` type in the workload. This is a hard requirement imposed by the driver. 

Focussing on `ReadOperationHandler`, let's take a closer look. 
The generic type parameter from the class definition (`ReadOperationHandler extends OperationHandler<ReadOperation>`) tells us that the handler takes a `ReadOperation` as input.

Looking more closely, the `executeOperation(ReadOperation operation)` method breaks down as follows.
First, the parameters are extracted from the `ReadOperation` instance and repackaged into the format expected by the system under test, in this case a `Map`.

Next, the vendor-specific database connection `client` (more details below) is retrieved via the `OperationHandler.dbConnectionState()` method (which in turn calls `Db.dbConnectionState()`). 
This is the recommended way of accessing shared state from within the `executeOperation()` method.
Then using `client` the operation is processed (e.g., by sending a query to a remote database server) and a result obtained (`Object result = client.execute(queryString, queryParams)`).

Finally, `ReadOperationHandler` returns the result, which is an `Integer` (remember, `ReadOperation extends Operation<Integer>`). This is done via a convenience method provided by `Operation`, `Operation.buildResult(int resultCode, RESULT_TYPE result)`. 

Note, `resultCode` is an optional parameter. Its sole purpose to be used as a debugging aid. See the [Reading benchmark results](Reading-Benchmark-Results.md) section for more details.            

For details, see [Implementing workload operations](Implementing-Workload-Operations.md).

##### Db
When extending `Db` three methods must be implemented: `onInit(Map<String, String> properties)`, `onCleanup()`, and `DbConnectionState getConnectionState()`.

We'll leave `getConnectionState()` for now, as it's covered in the next section.

`onInit()` is called before the benchmark is run and has three purposes.
First, it is how configuration parameters are passed into `Db` implementations (see [Driver configuration](Driver-Configuration.md) section for more details).
Second, any initializations (e.g., of a connection pool) should be done here.
Third, all `OperationHandler` implementations must be registered here using `registerOperationHandler()` method, e.g., `registerOperationHandler(ReadOperation.class, ReadOperationHandler.class)`.
Internally, `Db` maintains a mapping from `Operation` types to `OperationHandler` types and uses this to instantiate the correct `OperationHandler` instances at runtime, when the driver requests them.

`onCleanup()` is called after the benchmark has completed. Any resources that need to be released should be released here.

##### DbConnectionState
As previously stated, any state that needs to be shared between `OperationHandler` instances should be encapsulated in a subclass of `DbConnectionState`.
In this example we create a makeshift database connection client and store this within `BasicDbConnectionState`.
This client is then accessible via `getConnectionState()`. Though our example client does absolutely nothing, it is recommended that the pattern used to retrieve it is followed.
