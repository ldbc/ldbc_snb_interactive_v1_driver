package com.ldbc.db2;

import java.util.HashMap;
import java.util.Map;

public abstract class Db2
{
    private final Map<Class<?>, OperationHandler2<?>> operationHandlers = new HashMap<Class<?>, OperationHandler2<?>>();

    /**
     * Called once to initialize state for DB client
     */
    public abstract void init( Map<String, String> properties ) throws DbException2;

    /**
     * Called once to cleanup state for DB client
     */
    public abstract void cleanup() throws DbException2;

    protected final <A extends Operation2<?>, H extends OperationHandler2<A>> void registerOperationHandler(
            Class<A> operation, H operationHandler ) throws DbException2
    {
        if ( null != operationHandlers.get( operation ) )
        {
            throw new DbException2(
                    String.format( "Client already has handler registered for %s", operation.getClass() ) );
        }
        operationHandlers.put( operation, operationHandler );
    }

    public final OperationHandler2<?> getOperationHandler( Operation2<?> operation ) throws DbException2
    {
        OperationHandler2<?> operationHandler = operationHandlers.get( operation );
        if ( null == operationHandler )
        {
            throw new DbException2( String.format( "Client has no handler registered for %s", operation.getClass() ) );
        }
        return operationHandler;
    }

}
