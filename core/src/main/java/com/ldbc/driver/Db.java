package com.ldbc.driver;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.ldbc.driver.util.ClassLoaderHelper;

public abstract class Db
{
    private final Map<Class<? extends Operation<?>>, Class<? extends OperationHandler<?>>> operationHandlers = new HashMap<Class<? extends Operation<?>>, Class<? extends OperationHandler<?>>>();
    private boolean isInitialized = false;
    private boolean isCleanedUp = false;

    public final void init( Map<String, String> properties ) throws DbException
    {
        if ( true == isInitialized )
        {
            throw new DbException( "DB may be initialized only once" );
        }
        isInitialized = true;
        onInit( properties );
    }

    /**
     * Called once to initialize state for DB client
     */
    protected abstract void onInit( Map<String, String> properties ) throws DbException;

    public final void cleanup() throws DbException
    {
        if ( true == isCleanedUp )
        {
            throw new DbException( "DB may be cleaned up only once" );
        }
        isCleanedUp = true;
        onCleanup();
    }

    /**
     * Called once to cleanup state for DB client
     */
    protected abstract void onCleanup() throws DbException;

    public final <A extends Operation<?>, H extends OperationHandler<A>> void registerOperationHandler(
            Class<A> operationType, Class<H> operationHandlerType ) throws DbException
    {
        if ( null != operationHandlers.get( operationType ) )
        {
            throw new DbException( String.format( "Client already has handler registered for %s",
                    operationType.getClass() ) );
        }
        operationHandlers.put( operationType, operationHandlerType );
    }

    public final OperationHandler<?> getOperationHandler( Operation<?> operation ) throws DbException
    {
        Class<? extends OperationHandler<?>> operationHandlerType = operationHandlers.get( operation.getClass() );
        if ( null == operationHandlerType )
        {
            throw new DbException( String.format( "No handler registered for %s", operation.getClass() ) );
        }
        try
        {
            // TODO reassign from object pool rather than always new instances?
            OperationHandler<?> operationHandler = ClassLoaderHelper.loadOperationHandler( operationHandlerType,
                    operation );
            operationHandler.setDbConnectionState( getConnectionState() );
            return operationHandler;
        }
        catch ( Exception e )
        {
            throw new DbException( String.format( "Unable to instantiate handler %s", operationHandlerType ) );
        }
    }

    /**
     * Should return any state related to the database connection that can be
     * reused by all operation handlers
     */
    protected abstract DbConnectionState getConnectionState() throws DbException;

}
