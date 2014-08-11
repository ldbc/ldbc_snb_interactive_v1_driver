package com.ldbc.driver;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Db {
    private boolean isInitialized = false;
    private AtomicBoolean isCleanedUp = new AtomicBoolean(false);
    private final Map<Class<? extends Operation<?>>, OperationHandlerFactory> operationHandlerFactories = new HashMap<>();

    public final void init(Map<String, String> properties) throws DbException {
        if (true == isInitialized) {
            throw new DbException("DB may be initialized only once");
        }
        isInitialized = true;
        onInit(properties);
    }

    /**
     * Called once to initialize state for DB client
     */
    protected abstract void onInit(Map<String, String> properties) throws DbException;

    synchronized public final void cleanup() throws DbException {
        if (isCleanedUp.get()) {
            throw new DbException("DB may be cleaned up only once");
        }
        isCleanedUp.set(true);
        for (OperationHandlerFactory operationHandlerFactory : operationHandlerFactories.values()) {
            try {
                operationHandlerFactory.shutdown();
            } catch (OperationException e) {
                throw new DbException(
                        "Error shutting down operation handler factory - unclean shutdown: " + operationHandlerFactory.toString(),
                        e);
            }
        }
        onCleanup();
    }

    /**
     * Called once to cleanup state for DB client
     */
    protected abstract void onCleanup() throws DbException;

    public final <A extends Operation<?>, H extends OperationHandler<A>> void registerOperationHandler(Class<A> operationType, Class<H> operationHandlerType) throws DbException {
        if (operationHandlerFactories.containsKey(operationType))
            throw new DbException(String.format("Client already has handler registered for %s", operationType.getClass()));
        ReflectionOperationHandlerFactory reflectionOperationHandlerFactory = new ReflectionOperationHandlerFactory(operationHandlerType);
        PoolingOperationHandlerFactory poolingOperationHandlerFactory = new PoolingOperationHandlerFactory(reflectionOperationHandlerFactory);
        operationHandlerFactories.put(operationType, poolingOperationHandlerFactory);
    }

    synchronized public final OperationHandler<?> getOperationHandler(Operation<?> operation) throws DbException {
        OperationHandlerFactory operationHandlerFactory = operationHandlerFactories.get(operation.getClass());
        if (null == operationHandlerFactory)
            throw new DbException(String.format("No handler registered for %s", operation.getClass()));

        try {
            OperationHandler<?> operationHandler = operationHandlerFactory.newOperationHandler();
            operationHandler.setDbConnectionState(getConnectionState());
            return operationHandler;
        } catch (Exception e) {
            throw new DbException(String.format("Unable to instantiate handler for operation:\n%s", operation), e);
        }
    }

    /**
     * Should return any state related to the database connection that can be
     * reused by all operation handlers
     */
    protected abstract DbConnectionState getConnectionState() throws DbException;
}
