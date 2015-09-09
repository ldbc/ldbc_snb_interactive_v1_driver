package com.ldbc.driver;

import com.google.common.collect.Ordering;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.util.ClassLoaderHelper;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

public abstract class Db implements Closeable
{
    private boolean isInitialized = false;
    private AtomicBoolean isShutdown = new AtomicBoolean( false );
    private DbConnectionState dbConnectionState = null;
    private Map<Class<? extends Operation>,OperationHandler> operationHandlers = new HashMap<>();
    private OperationHandler[] operationHandlersArray = null;
    private OperationHandlerRunnerFactory operationHandlerRunnableContextFactory = null;

    synchronized public final void init(
            Map<String,String> params,
            LoggingService loggingService,
            Map<Integer,Class<? extends Operation>> operationTypeToClassMapping )
            throws DbException
    {
        if ( true == isInitialized )
        {
            throw new DbException( "DB may be initialized only once" );
        }
        onInit( params, loggingService );
        dbConnectionState = getConnectionState();
        operationHandlerRunnableContextFactory = new PoolingOperationHandlerRunnerFactory(
                new InstantiatingOperationHandlerRunnerFactory()
        );
        operationHandlersArray = toOperationHandlerArray( operationTypeToClassMapping, operationHandlers );
        operationHandlers = null;
        isInitialized = true;
    }

    /**
     * Called once to initialize state for DB client
     */
    protected abstract void onInit( Map<String,String> properties, LoggingService loggingService ) throws DbException;

    @Override
    synchronized public final void close() throws IOException
    {
        if ( isShutdown.get() )
        {
            throw new IOException( "DB may be cleaned up only once" );
        }
        isShutdown.set( true );
        onClose();
        try
        {
            operationHandlerRunnableContextFactory.shutdown();
        }
        catch ( OperationException e )
        {
            throw new IOException( "Error shutting down operation handler runnable factory", e );
        }
    }

    // TODO this is a temporary hack to support warmup more easily, because the runnable contexts need to be cleared
    // TODO ultimately this would be done in another way
    synchronized public final void reInit() throws DbException
    {
        try
        {
            operationHandlerRunnableContextFactory.shutdown();
        }
        catch ( OperationException e )
        {
            throw new DbException( "Error shutting down operation handler runnable factory", e );
        }
        operationHandlerRunnableContextFactory = new PoolingOperationHandlerRunnerFactory(
                new InstantiatingOperationHandlerRunnerFactory()
        );
    }

    /**
     * Called once to cleanup state for DB client
     */
    protected abstract void onClose() throws IOException;

    public final <A extends Operation, H extends OperationHandler<A,?>> void registerOperationHandler(
            Class<A> operationType, Class<H> operationHandlerType ) throws DbException
    {
        if ( operationHandlers.containsKey( operationType ) )
        {
            throw new DbException( format( "Client already has handler registered for %s", operationType.getClass() ) );
        }
        try
        {
            OperationHandler operationHandler = ClassLoaderHelper.loadOperationHandler( operationHandlerType );
            operationHandlers.put( operationType, operationHandler );
        }
        catch ( OperationException e )
        {
            throw new DbException(
                    format( "%s could not instantiate instance of %s",
                            getClass().getSimpleName(),
                            operationHandlerType.getSimpleName()
                    ),
                    e );
        }
    }

    public final OperationHandlerRunnableContext getOperationHandlerRunnableContext( Operation operation )
            throws DbException
    {
        OperationHandler operationHandler = operationHandlersArray[operation.type()];
        if ( null == operationHandler )
        {
            throw new DbException( format( "No handler registered for %s", operation.getClass() ) );
        }
        try
        {
            OperationHandlerRunnableContext operationHandlerRunnableContext =
                    operationHandlerRunnableContextFactory.newOperationHandlerRunner();
            operationHandlerRunnableContext.setOperationHandler( operationHandler );
            operationHandlerRunnableContext.setDbConnectionState( dbConnectionState );
            return operationHandlerRunnableContext;
        }
        catch ( Exception e )
        {
            throw new DbException( format( "Unable to instantiate handler for operation:\n%s", operation ), e );
        }
    }

    private static OperationHandler[] toOperationHandlerArray(
            Map<Integer,Class<? extends Operation>> operationTypeToClassMapping,
            Map<Class<? extends Operation>,OperationHandler> operationHandlers ) throws DbException
    {
        if ( operationTypeToClassMapping.isEmpty() )
        {
            return new OperationHandler[]{};
        }
        else
        {
            int minOperationType = Ordering.<Integer>natural().min( operationTypeToClassMapping.keySet() );
            if ( minOperationType < 0 )
            {
                throw new DbException( format( "Operation type code lower than 0: %s", minOperationType ) );
            }

            int maxOperationType = Ordering.<Integer>natural().max( operationTypeToClassMapping.keySet() );
            OperationHandler[] operationHandlersArray = new OperationHandler[maxOperationType + 1];
            for ( int i = 0; i < operationHandlersArray.length; i++ )
            {
                if ( operationTypeToClassMapping.containsKey( i ) )
                {
                    operationHandlersArray[i] = operationHandlers.get( operationTypeToClassMapping.get( i ) );
                }
            }

            return operationHandlersArray;
        }
    }

    /**
     * Should return any state related to the database connection that can be
     * reused by all operation handlers
     */
    protected abstract DbConnectionState getConnectionState() throws DbException;
}
