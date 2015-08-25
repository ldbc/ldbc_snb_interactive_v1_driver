package com.ldbc.driver.workloads.dummy;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.control.LoggingService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DummyDb extends Db
{
    private class AllowedConnectionState extends DbConnectionState
    {
        private final Map<String,Boolean> nameAllowedMap;
        private boolean defaultAllowed;

        private AllowedConnectionState( boolean defaultAllowed )
        {
            this.defaultAllowed = defaultAllowed;
            this.nameAllowedMap = new HashMap<>();
        }

        private void setNameAllowedValue( String name, boolean allowed )
        {
            nameAllowedMap.put( name, allowed );
        }

        private void setAllowedValueForAll( boolean allowed )
        {
            nameAllowedMap.clear();
            defaultAllowed = allowed;
        }

        private boolean isAllowed( String name )
        {
            if ( false == nameAllowedMap.containsKey( name ) )
            { return defaultAllowed; }
            return nameAllowedMap.get( name );
        }

        @Override
        public void close() throws IOException
        {

        }
    }

    public static final String ALLOWED_DEFAULT_ARG = "allowed";
    private static final boolean ALLOWED_DEFAULT = true;

    private AllowedConnectionState allowedConnectionState = null;

    public void setNameAllowedValue( String name, boolean allowed )
    {
        allowedConnectionState.setNameAllowedValue( name, allowed );
    }

    public void setAllowedValueForAll( boolean allowed )
    {
        allowedConnectionState.setAllowedValueForAll( allowed );
    }

    @Override
    protected void onInit( Map<String,String> params, LoggingService loggingService ) throws DbException
    {
        registerOperationHandler( NothingOperation.class, NothingOperationHandler.class );
        registerOperationHandler( TimedNamedOperation1.class, TimedNamedOperation1Handler.class );
        registerOperationHandler( TimedNamedOperation2.class, TimedNamedOperation2Handler.class );
        registerOperationHandler( TimedNamedOperation3.class, TimedNamedOperation3Handler.class );
        boolean allowedDefault = (params.containsKey( ALLOWED_DEFAULT_ARG ))
                                 ? Boolean.parseBoolean( params.get( ALLOWED_DEFAULT_ARG ) )
                                 : ALLOWED_DEFAULT;
        allowedConnectionState = new AllowedConnectionState( allowedDefault );
    }

    @Override
    protected void onClose() throws IOException
    {
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException
    {
        return allowedConnectionState;
    }

    public static class NothingOperationHandler implements OperationHandler<NothingOperation,AllowedConnectionState>
    {
        @Override
        public void executeOperation( NothingOperation operation, AllowedConnectionState connectionState,
                ResultReporter resultReporter ) throws DbException
        {
            resultReporter.report( 0, new DummyResult(), operation );
        }
    }

    public static class TimedNamedOperation1Handler
            implements OperationHandler<TimedNamedOperation1,AllowedConnectionState>
    {
        @Override
        public void executeOperation( TimedNamedOperation1 operation, AllowedConnectionState connectionState,
                ResultReporter resultReporter ) throws DbException
        {
            while ( false == connectionState.isAllowed( operation.name() ) )
            {
                // wait to be a allowed to execute
            }
            resultReporter.report( 0, new DummyResult(), operation );
        }
    }

    public static class TimedNamedOperation2Handler
            implements OperationHandler<TimedNamedOperation2,AllowedConnectionState>
    {
        @Override
        public void executeOperation( TimedNamedOperation2 operation, AllowedConnectionState connectionState,
                ResultReporter resultReporter ) throws DbException
        {
            while ( false == connectionState.isAllowed( operation.name() ) )
            {
                // wait to be a allowed to execute
            }
            resultReporter.report( 0, new DummyResult(), operation );
        }
    }

    public static class TimedNamedOperation3Handler
            implements OperationHandler<TimedNamedOperation3,AllowedConnectionState>
    {
        @Override
        public void executeOperation( TimedNamedOperation3 operation, AllowedConnectionState connectionState,
                ResultReporter resultReporter ) throws DbException
        {
            while ( false == connectionState.isAllowed( operation.name() ) )
            {
                // wait to be a allowed to execute
            }
            resultReporter.report( 0, new DummyResult(), operation );
        }
    }
}
