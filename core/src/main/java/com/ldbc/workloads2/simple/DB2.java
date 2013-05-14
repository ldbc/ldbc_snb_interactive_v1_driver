package com.ldbc.workloads2.simple;

import java.util.HashMap;
import java.util.Map;

import com.ldbc.DBException;
import com.ldbc.workloads2.OperationArgs2;
import com.ldbc.workloads2.OperationHandler2;

public abstract class DB2
{
    private final Map<String, String> properties;
    private final Map<OperationArgs2, OperationHandler2<?>> operationHandlers = new HashMap<OperationArgs2, OperationHandler2<?>>();

    DB2( Map<String, String> properties )
    {
        this.properties = properties;
    }

    public final Map<String, String> getProperties()
    {
        return properties;
    }

    /**
     * Called once to initialize state for DB client
     */
    public abstract void init() throws DBException;

    /**
     * Called once to cleanup state for DB client
     */
    public abstract void cleanup() throws DBException;

    public final <A extends OperationArgs2, H extends OperationHandler2<A>> void registerOperationHandler(
            A operationArgs, H operationHandler )
    {
        operationHandlers.put( operationArgs, operationHandler );
    }

    public final OperationHandler2<?> getOperationHandler( OperationArgs2 operationArgs ) throws DBException
    {
        OperationHandler2<?> operationHandler = operationHandlers.get( operationArgs );
        if ( null == operationHandler )
        {
            throw new DBException( String.format( "Client has no handler registered for %s", operationArgs.getClass() ) );
        }
        return operationHandler;
    }

}
