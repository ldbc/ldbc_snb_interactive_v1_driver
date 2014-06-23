package com.ldbc.driver.workloads.simple.db;

import com.ldbc.driver.*;
import com.ldbc.driver.data.ByteIterator;
import com.ldbc.driver.workloads.simple.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

public class BasicDb extends Db {
    static class BasicClient {
        BasicClient(String connectionUrl) {
        }

        Object execute(String queryString, Map<String, Object> queryParams) {
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
        registerOperationHandler(InsertOperation.class, InsertOperationHandler.class);
        registerOperationHandler(ReadOperation.class, ReadOperationHandler.class);
        registerOperationHandler(UpdateOperation.class, UpdateOperationHandler.class);
        registerOperationHandler(ScanOperation.class, ScanOperationHandler.class);
        registerOperationHandler(ReadModifyWriteOperation.class, ReadModifyWriteOperationHandler.class);

        String connectionUrl = properties.get("url");
        connectionState = new BasicDbConnectionState(connectionUrl);
    }

    @Override
    public void onCleanup() throws DbException {
    }

    public static class InsertOperationHandler extends OperationHandler<InsertOperation> {
        @Override
        public OperationResult executeOperation(InsertOperation operation) {
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("table", operation.getTable());
            queryParams.put("key", operation.getKey());
            queryParams.put("valuedFields", operation.getValuedFields());

            // TODO replace with actual query string
            String queryString = null;

            BasicClient client = ((BasicDbConnectionState) dbConnectionState()).client();
            Object result = client.execute(queryString, queryParams);

            // TODO replace with actual result code
            return operation.buildResult(0, result);
        }
    }

    public static class ReadOperationHandler extends OperationHandler<ReadOperation> {
        @Override
        public OperationResult executeOperation(ReadOperation operation) {
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("table", operation.getTable());
            queryParams.put("key", operation.getKey());
            queryParams.put("fields", operation.getFields());

            // TODO replace with actual query string
            String queryString = null;

            BasicClient client = ((BasicDbConnectionState) dbConnectionState()).client();
            Map<String, ByteIterator> result = (Map<String, ByteIterator>) client.execute(queryString, queryParams);

            // TODO replace with actual result code
            return operation.buildResult(0, result);
        }
    }

    public static class UpdateOperationHandler extends OperationHandler<UpdateOperation> {
        @Override
        public OperationResult executeOperation(UpdateOperation operation) {
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("table", operation.getTable());
            queryParams.put("key", operation.getKey());
            queryParams.put("values", operation.getValues());

            // TODO replace with actual query string
            String queryString = null;

            Map<String, ByteIterator> result =
                    (Map<String, ByteIterator>) ((BasicDbConnectionState) dbConnectionState()).client().execute(queryString, queryParams);

            // TODO replace with actual result code
            return operation.buildResult(0, result);
        }
    }

    public static class ScanOperationHandler extends OperationHandler<ScanOperation> {
        @Override
        public OperationResult executeOperation(ScanOperation operation) {
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("table", operation.getTable());
            queryParams.put("key", operation.getStartkey());
            queryParams.put("recordCount", operation.getRecordcount());
            queryParams.put("fields", operation.getFields());

            // TODO replace with actual query string
            String queryString = null;

            Vector<Map<String, ByteIterator>> result =
                    (Vector<Map<String, ByteIterator>>) ((BasicDbConnectionState) dbConnectionState()).client().execute(queryString, queryParams);

            // TODO replace with actual result code
            return operation.buildResult(0, result);
        }
    }

    public static class ReadModifyWriteOperationHandler extends OperationHandler<ReadModifyWriteOperation> {
        @Override
        public OperationResult executeOperation(ReadModifyWriteOperation operation) {
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("table", operation.getTable());
            queryParams.put("key", operation.getKey());
            queryParams.put("fields", operation.getFields());
            queryParams.put("values", operation.getValues());

            // TODO replace with actual query string
            String queryString = null;

            Object result =
                    ((BasicDbConnectionState) dbConnectionState()).client().execute(queryString, queryParams);

            // TODO replace with actual result code
            return operation.buildResult(0, result);
        }
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return connectionState;
    }
}
