package com.ldbc.driver;

public interface OperationHandler<OPERATION_TYPE extends Operation, DB_CONNECTION_STATE_TYPE extends DbConnectionState> {
    void executeOperation(OPERATION_TYPE operation, DB_CONNECTION_STATE_TYPE dbConnectionState, ResultReporter resultReporter) throws DbException;
}
