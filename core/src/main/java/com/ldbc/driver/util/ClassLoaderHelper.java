package com.ldbc.driver.util;

import java.io.OutputStream;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.measurements.MeasurementsException;
import com.ldbc.driver.measurements.exporter.MeasurementsExporter;
import com.ldbc.driver.workloads.Workload;
import com.ldbc.driver.workloads.WorkloadException;

// TODO test
public class ClassLoaderHelper
{

    /**
     * DB
     */
    public static Db loadDb( String dbClassName ) throws DbException
    {
        try
        {
            return loadDb( loadClass( dbClassName, Db.class ) );
        }
        catch ( ClassNotFoundException e )
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
        catch ( ClassNotFoundException e )
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
        catch ( ClassNotFoundException e )
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
            String errMsg = String.format( "Error creating OperationHandler [%s] with Operation [%s]",
                    operationHandlerClass.getName(), operation.getClass().getName() );
            throw new OperationException( errMsg, e.getCause() );
        }
    }

    /**
     * MeasurementsExporter
     */
    public static MeasurementsExporter loadMeasurementsExporter( String measurementsExporterClassName, OutputStream out )
            throws MeasurementsException
    {
        try
        {
            return loadMeasurementsExporter( loadClass( measurementsExporterClassName, MeasurementsExporter.class ),
                    out );
        }
        catch ( ClassNotFoundException e )
        {
            String errMsg = String.format( "Error creating MeasurementsExporter [%s] with OutputStream [%s]",
                    measurementsExporterClassName, out.getClass().getName() );
            throw new MeasurementsException( errMsg, e.getCause() );
        }
    }

    public static MeasurementsExporter loadMeasurementsExporter(
            Class<? extends MeasurementsExporter> measurementsExporterClass, OutputStream out )
            throws MeasurementsException
    {
        try
        {
            return measurementsExporterClass.getConstructor( OutputStream.class ).newInstance( out );
        }
        catch ( Exception e )
        {
            String errMsg = String.format( "Error creating MeasurementsExporter [%s] with OutputStream [%s]",
                    measurementsExporterClass.getName(), out.getClass().getName() );
            throw new MeasurementsException( errMsg, e.getCause() );
        }
    }

    /**
     * Helper Methods
     */
    private static <C> Class<? extends C> loadClass( String className, Class<C> baseClass )
            throws ClassNotFoundException
    {
        ClassLoader classLoader = ClassLoaderHelper.class.getClassLoader();
        return (Class<? extends C>) classLoader.loadClass( className );
    }

}
