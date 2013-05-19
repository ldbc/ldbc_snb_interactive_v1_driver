package com.ldbc.db2;

import java.util.HashMap;
import java.util.Map;

public abstract class Db2
{
    private final Map<Class<? extends Operation2<?>>, Class<? extends OperationHandler2<?>>> operationHandlers;
    private boolean isInitialized;
    private boolean isCleanedUp;

    public Db2()
    {
        this.operationHandlers = new HashMap<Class<? extends Operation2<?>>, Class<? extends OperationHandler2<?>>>();
        this.isInitialized = false;
        this.isCleanedUp = false;
    }

    public final void init( Map<String, String> properties ) throws DbException2
    {
        if ( true == isInitialized )
        {
            throw new DbException2( "DB may be initialized only once" );
        }
        isInitialized = true;
        onInit( properties );
    }

    /**
     * Called once to initialize state for DB client
     */
    protected abstract void onInit( Map<String, String> properties ) throws DbException2;

    public final void cleanup() throws DbException2
    {
        if ( true == isCleanedUp )
        {
            throw new DbException2( "DB may be cleaned up only once" );
        }
        isCleanedUp = true;
        onCleanup();
    }

    /**
     * Called once to cleanup state for DB client
     */
    protected abstract void onCleanup() throws DbException2;

    protected final <A extends Operation2<?>, H extends OperationHandler2<A>> void registerOperationHandler(
            Class<A> operationType, Class<H> operationHandlerType ) throws DbException2
    {
        if ( null != operationHandlers.get( operationType ) )
        {
            throw new DbException2( String.format( "Client already has handler registered for %s",
                    operationType.getClass() ) );
        }
        operationHandlers.put( operationType, operationHandlerType );
    }

    public final OperationHandler2<?> getOperationHandler( Operation2<?> operation ) throws DbException2
    {
        Class<? extends OperationHandler2<?>> operationHandlerType = (Class<? extends OperationHandler2<?>>) operationHandlers.get( operation.getClass() );
        if ( null == operationHandlerType )
        {
            throw new DbException2( String.format( "No handler registered for %s", operation.getClass() ) );
        }
        try
        {
            return operationHandlerType.getConstructor().newInstance();
        }
        catch ( Exception e )
        {
            throw new DbException2( String.format( "Unable to instantiate handler %s", operationHandlerType ) );
        }
    }
}
