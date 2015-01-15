package com.ldbc.driver;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Db implements Closeable {
    private boolean isInitialized = false;
    private AtomicBoolean isShutdown = new AtomicBoolean(false);
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

    @Override
    synchronized public final void close() throws IOException {
        if (isShutdown.get()) {
            throw new IOException("DB may be cleaned up only once");
        }
        isShutdown.set(true);
        for (OperationHandlerFactory operationHandlerFactory : operationHandlerFactories.values()) {
            try {
                operationHandlerFactory.shutdown();
            } catch (OperationException e) {
                throw new IOException(
                        "Error shutting down operation handler factory - unclean shutdown: " + operationHandlerFactory.toString(),
                        e);
            }
        }
        onClose();
    }

    /**
     * Called once to cleanup state for DB client
     */
    protected abstract void onClose() throws IOException;

    public final <A extends Operation<?>, H extends OperationHandler<A>> void registerOperationHandler(Class<A> operationType, Class<H> operationHandlerType) throws DbException {
        if (operationHandlerFactories.containsKey(operationType))
            throw new DbException(String.format("Client already has handler registered for %s", operationType.getClass()));
        ReflectionOperationHandlerFactory reflectionOperationHandlerFactory = new ReflectionOperationHandlerFactory(operationHandlerType);
        PoolingOperationHandlerFactory poolingOperationHandlerFactory = new PoolingOperationHandlerFactory(reflectionOperationHandlerFactory);
        operationHandlerFactories.put(operationType, poolingOperationHandlerFactory);
    }

    public final OperationHandler<?> getOperationHandler(Operation<?> operation) throws DbException {
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
