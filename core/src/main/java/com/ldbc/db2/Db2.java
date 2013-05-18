package com.ldbc.db2;

import java.util.HashMap;
import java.util.Map;

public abstract class Db2
{
    private final Map<Class<? extends Operation2<?>>, Class<? extends OperationHandler2<?>>> operationHandlers = new HashMap<Class<? extends Operation2<?>>, Class<? extends OperationHandler2<?>>>();

    /**
     * Called once to initialize state for DB client
     */
    public abstract void init( Map<String, String> properties ) throws DbException2;

    /**
     * Called once to cleanup state for DB client
     */
    public abstract void cleanup() throws DbException2;

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

    // protected final <A extends Operation2<?>, H extends OperationHandler2<A>>
    // void registerOperationHandler(
    // Class<A> operation, H operationHandler ) throws DbException2
    // {
    // if ( null != operationHandlers.get( operation ) )
    // {
    // throw new DbException2(
    // String.format( "Client already has handler registered for %s",
    // operation.getClass() ) );
    // }
    // operationHandlers.put( operation, operationHandler );
    // }

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
    // public final OperationHandler2<?> getOperationHandler( Operation2<?>
    // operation ) throws DbException2
    // {
    // OperationHandler2<?> operationHandler = operationHandlers.get(
    // operation.getClass() );
    // if ( null == operationHandler )
    // {
    // throw new DbException2( String.format(
    // "Client has no handler registered for %s", operation.getClass() ) );
    // }
    // return operationHandler;
    // }

}
