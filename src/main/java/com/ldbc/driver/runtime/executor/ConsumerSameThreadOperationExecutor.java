package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.Db;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandlerRunnableContext;

import static java.lang.String.format;

public class ConsumerSameThreadOperationExecutor implements OperationExecutor {
    Db db;

    public ConsumerSameThreadOperationExecutor( Db db) {
        this.db = db;
    }

    @Override
    public final void execute( Operation operation ) throws OperationExecutorException {
        OperationHandlerRunnableContext context;
        try {
            context = db.getOperationHandlerRunnableContext( operation );
        } catch (Exception e) {
            throw new OperationExecutorException(
                format( "Error while retrieving handler for operation\nOperation: %s", operation ), e );
        }
        context.cleanup();
    }

    @Override
    synchronized public final void shutdown( long waitAsMilli ) throws OperationExecutorException {
    }

    @Override
    public long uncompletedOperationHandlerCount() {
        return 1;
    }
}
