package com.ldbc.driver;

public abstract class OperationHandler<OPERATION_TYPE extends Operation<?>, DB_CONNECTION_STATE_TYPE extends DbConnectionState> {
    public abstract void executeOperation(OPERATION_TYPE operation, DB_CONNECTION_STATE_TYPE dbConnectionState, ResultReporter resultReporter) throws DbException;

    @Override
    public String toString() {
        return String.format("OperationHandler [type=%s]", getClass().getName());
    }
}
