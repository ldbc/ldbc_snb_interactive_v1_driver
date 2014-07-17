package com.ldbc.driver.workloads.dummy;

import com.ldbc.driver.*;

import java.util.HashMap;
import java.util.Map;

public class DummyDb extends Db {
    private class AllowedConnectionState extends DbConnectionState {
        private final Map<String, Boolean> nameAllowedMap;
        private final boolean defaultAllowed;

        private AllowedConnectionState(boolean defaultAllowed) {
            this.defaultAllowed = defaultAllowed;
            nameAllowedMap = new HashMap<>();
        }

        private void setNameAllowedValue(String name, Boolean allowed) {
            nameAllowedMap.put(name, allowed);
        }

        private boolean isAllowed(String name) {
            if (false == nameAllowedMap.containsKey(name)) return defaultAllowed;
            return nameAllowedMap.get(name);
        }
    }

    public static final String ALLOWED_ARG = "allowed";
    private static final boolean ALLOWED_DEFAULT = true;

    private AllowedConnectionState allowedConnectionState = null;

    public void setNameAllowedValue(String name, Boolean allowed) {
        allowedConnectionState.setNameAllowedValue(name, allowed);
    }

    @Override
    protected void onInit(Map<String, String> params) throws DbException {
        registerOperationHandler(NothingOperation.class, NothingOperationHandler.class);
        registerOperationHandler(TimedNamedOperation1.class, TimedNamedOperation1Handler.class);
        registerOperationHandler(TimedNamedOperation2.class, TimedNamedOperation2Handler.class);
        boolean allowedDefault = (params.containsKey(ALLOWED_ARG))
                ? Boolean.parseBoolean(params.get(ALLOWED_ARG))
                : ALLOWED_DEFAULT;
        allowedConnectionState = new AllowedConnectionState(allowedDefault);
    }

    @Override
    protected void onCleanup() throws DbException {
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return allowedConnectionState;
    }

    public static class NothingOperationHandler extends OperationHandler<NothingOperation> {
        @Override
        protected OperationResultReport executeOperation(NothingOperation operation) throws DbException {
            return operation.buildResult(0, new DummyResult());
        }
    }

    public static class TimedNamedOperation1Handler extends OperationHandler<TimedNamedOperation1> {
        @Override
        protected OperationResultReport executeOperation(TimedNamedOperation1 operation) throws DbException {
            AllowedConnectionState connectionState = (AllowedConnectionState) dbConnectionState();
            while (false == connectionState.isAllowed(operation.name())) {
                // wait to be a allowed to execute
            }
            return operation.buildResult(0, new DummyResult());
        }
    }

    public static class TimedNamedOperation2Handler extends OperationHandler<TimedNamedOperation2> {
        @Override
        protected OperationResultReport executeOperation(TimedNamedOperation2 operation) throws DbException {
            AllowedConnectionState connectionState = (AllowedConnectionState) dbConnectionState();
            while (false == connectionState.isAllowed(operation.name())) {
                // wait to be a allowed to execute
            }
            return operation.buildResult(0, new DummyResult());
        }
    }
}
