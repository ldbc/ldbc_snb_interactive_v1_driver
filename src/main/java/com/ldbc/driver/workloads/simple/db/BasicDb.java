package com.ldbc.driver.workloads.simple.db;

import com.ldbc.driver.*;
import com.ldbc.driver.workloads.simple.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
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

        @Override
        public void close() throws IOException {

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
    public void onClose() throws IOException {
    }

    public static class InsertOperationHandler extends OperationHandler<InsertOperation, BasicDbConnectionState> {
        @Override
        public void executeOperation(InsertOperation operation, BasicDbConnectionState dbConnectionState, ResultReporter resultReporter) {
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("table", operation.getTable());
            queryParams.put("key", operation.getKey());
            queryParams.put("valuedFields", operation.getValuedFields());

            // TODO replace with actual query string
            String queryString = null;

            BasicClient client = dbConnectionState.client();
            Object result = client.execute(queryString, queryParams);

            // TODO replace with actual result code
            resultReporter.report(0, result, operation);
        }
    }

    public static class ReadOperationHandler extends OperationHandler<ReadOperation, BasicDbConnectionState> {
        @Override
        public void executeOperation(ReadOperation operation, BasicDbConnectionState dbConnectionState, ResultReporter resultReporter) {
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("table", operation.getTable());
            queryParams.put("key", operation.getKey());
            queryParams.put("fields", operation.getFields());

            // TODO replace with actual query string
            String queryString = null;

            BasicClient client = dbConnectionState.client();
            Map<String, Iterator<Byte>> result = (Map<String, Iterator<Byte>>) client.execute(queryString, queryParams);

            // TODO replace with actual result code
            resultReporter.report(0, result, operation);
        }
    }

    public static class UpdateOperationHandler extends OperationHandler<UpdateOperation, BasicDbConnectionState> {
        @Override
        public void executeOperation(UpdateOperation operation, BasicDbConnectionState dbConnectionState, ResultReporter resultReporter) {
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("table", operation.getTable());
            queryParams.put("key", operation.getKey());
            queryParams.put("values", operation.getValues());

            // TODO replace with actual query string
            String queryString = null;

            Map<String, Iterator<Byte>> result = (Map<String, Iterator<Byte>>) dbConnectionState.client().execute(queryString, queryParams);

            // TODO replace with actual result code
            resultReporter.report(0, result, operation);
        }
    }

    public static class ScanOperationHandler extends OperationHandler<ScanOperation, BasicDbConnectionState> {
        @Override
        public void executeOperation(ScanOperation operation, BasicDbConnectionState dbConnectionState, ResultReporter resultReporter) {
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("table", operation.getTable());
            queryParams.put("key", operation.getStartkey());
            queryParams.put("recordCount", operation.getRecordcount());
            queryParams.put("fields", operation.getFields());

            // TODO replace with actual query string
            String queryString = null;

            Vector<Map<String, Iterator<Byte>>> result = (Vector<Map<String, Iterator<Byte>>>) dbConnectionState.client().execute(queryString, queryParams);

            // TODO replace with actual result code
            resultReporter.report(0, result, operation);
        }
    }

    public static class ReadModifyWriteOperationHandler extends OperationHandler<ReadModifyWriteOperation, BasicDbConnectionState> {
        @Override
        public void executeOperation(ReadModifyWriteOperation operation, BasicDbConnectionState dbConnectionState, ResultReporter resultReporter) {
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("table", operation.getTable());
            queryParams.put("key", operation.getKey());
            queryParams.put("fields", operation.getFields());
            queryParams.put("values", operation.getValues());

            // TODO replace with actual query string
            String queryString = null;

            Object result = dbConnectionState.client().execute(queryString, queryParams);

            // TODO replace with actual result code
            resultReporter.report(0, result, operation);
        }
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return connectionState;
    }
}
