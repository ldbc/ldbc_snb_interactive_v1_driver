package com.ldbc.driver.util;

import org.apache.log4j.Logger;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;

// TODO test
public class ClassLoaderHelper
{
    private static Logger logger = Logger.getLogger( ClassLoaderHelper.class );

    /**
     * DB
     */
    public static Db loadDb( String dbClassName ) throws DbException
    {
        try
        {
            return loadDb( loadClass( dbClassName, Db.class ) );
        }
        catch ( ClassLoadingException e )
        {
            String errMsg = String.format( "Error creating DB [%s]", dbClassName );
            throw new DbException( errMsg, e.getCause() );
        }
    }

    public static Db loadDb( Class<? extends Db> dbClass ) throws DbException
    {
        try
        {
            return dbClass.getConstructor().newInstance();
        }
        catch ( Exception e )
        {
            String errMsg = String.format( "Error creating DB [%s]", dbClass.getName() );
            throw new DbException( errMsg, e.getCause() );
        }
    }

    /**
     * Workload
     */
    public static Workload loadWorkload( String workloadClassName ) throws WorkloadException
    {
        try
        {
            return loadWorkload( loadClass( workloadClassName, Workload.class ) );
        }
        catch ( ClassLoadingException e )
        {
            String errMsg = String.format( "Error creating Workload [%s]", workloadClassName );
            throw new WorkloadException( errMsg, e.getCause() );
        }
    }

    public static Workload loadWorkload( Class<? extends Workload> workloadClass ) throws WorkloadException
    {
        try
        {
            return workloadClass.getConstructor().newInstance();
        }
        catch ( Exception e )
        {
            String errMsg = String.format( "Error creating Workload [%s]", workloadClass.getName() );
            throw new WorkloadException( errMsg, e.getCause() );
        }
    }

    /**
     * OperationHandler
     */
    public static OperationHandler<?> loadOperationHandler( String operationHandlerClassName, Operation<?> operation )
            throws OperationException
    {
        try
        {
            return loadOperationHandler( loadClass( operationHandlerClassName, OperationHandler.class ), operation );
        }
        catch ( ClassLoadingException e )
        {
            String errMsg = String.format( "Error creating OperationHandler [%s] with Operation [%s]",
                    operationHandlerClassName, operation.getClass().getName() );
            throw new OperationException( errMsg, e.getCause() );
        }
    }

    public static OperationHandler<?> loadOperationHandler( Class<? extends OperationHandler> operationHandlerClass,
            Operation<?> operation ) throws OperationException
    {
        try
        {
            OperationHandler<?> operationHandler = operationHandlerClass.getConstructor().newInstance();
            operationHandler.setOperation( operation );
            return operationHandler;
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            String errMsg = String.format( "Error creating OperationHandler [%s] with Operation [%s]",
                    operationHandlerClass.getName(), operation.getClass().getName() );
            throw new OperationException( errMsg, e.getCause() );
        }
    }

    /**
     * Helper Methods
     */
    public static <C> Class<? extends C> loadClass( String className, Class<C> baseClass ) throws ClassLoadingException

    {
        try
        {
            ClassLoader classLoader = ClassLoaderHelper.class.getClassLoader();
            logger.debug( String.format( "Loading class [%s], descendant of [%s]", className, baseClass.getName() ) );
            Class<?> loadedClass = classLoader.loadClass( className );
            // Class<?> loadedClass = Class.forName(className,false,classLoader)
            logger.debug( String.format( "Loaded class [%s]", loadedClass.getName() ) );
            return (Class<? extends C>) loadedClass;
        }
        catch ( ClassNotFoundException e )
        {
            throw new ClassLoadingException( String.format( "Error loading class [%s]", className ), e.getCause() );
        }
    }
}
