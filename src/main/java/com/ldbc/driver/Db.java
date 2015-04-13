package com.ldbc.driver;

import com.ldbc.driver.util.ClassLoaderHelper;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Db implements Closeable {
    private boolean isInitialized = false;
    private AtomicBoolean isShutdown = new AtomicBoolean(false);
    private DbConnectionState dbConnectionState = null;
    private final Map<Class<? extends Operation>, OperationHandler> operationHandlers = new HashMap<>();
    private OperationHandlerRunnerFactory operationHandlerRunnableContextFactory = null;

    synchronized public final void init(Map<String, String> properties) throws DbException {
        if (true == isInitialized) {
            throw new DbException("DB may be initialized only once");
        }
        onInit(properties);
        dbConnectionState = getConnectionState();
        operationHandlerRunnableContextFactory = new PoolingOperationHandlerRunnerFactory(
                new InstantiatingOperationHandlerRunnerFactory()
        );
        isInitialized = true;
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
        onClose();
        try {
            operationHandlerRunnableContextFactory.shutdown();
        } catch (OperationException e) {
            throw new IOException(e);
        }
    }

    /**
     * Called once to cleanup state for DB client
     */
    protected abstract void onClose() throws IOException;

    public final <A extends Operation, H extends OperationHandler<A, ?>> void registerOperationHandler(Class<A> operationType, Class<H> operationHandlerType) throws DbException {
        if (operationHandlers.containsKey(operationType))
            throw new DbException(String.format("Client already has handler registered for %s", operationType.getClass()));
        try {
            OperationHandler operationHandler = ClassLoaderHelper.loadOperationHandler(operationHandlerType);
            operationHandlers.put(operationType, operationHandler);
        } catch (OperationException e) {
            throw new DbException(
                    String.format("%s could not instantiate instance of %s",
                            getClass().getSimpleName(),
                            operationHandlerType.getSimpleName()
                    ),
                    e);
        }
    }

    public final OperationHandlerRunnableContext getOperationHandlerRunnableContext(Operation operation) throws DbException {
        OperationHandler operationHandler = operationHandlers.get(operation.getClass());
        if (null == operationHandler)
            throw new DbException(String.format("No handler registered for %s", operation.getClass()));

        try {
            OperationHandlerRunnableContext operationHandlerRunnableContext = operationHandlerRunnableContextFactory.newOperationHandlerRunner();
            operationHandlerRunnableContext.setOperationHandler(operationHandler);
            operationHandlerRunnableContext.setDbConnectionState(dbConnectionState);
            return operationHandlerRunnableContext;
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
