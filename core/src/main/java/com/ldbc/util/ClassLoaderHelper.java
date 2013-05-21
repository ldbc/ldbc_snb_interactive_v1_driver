package com.ldbc.util;

import java.io.OutputStream;

import com.ldbc.Db;
import com.ldbc.DbException;
import com.ldbc.measurements.MeasurementsException;
import com.ldbc.measurements.exporter.MeasurementsExporter;
import com.ldbc.workloads.Workload;
import com.ldbc.workloads.WorkloadException;

public class ClassLoaderHelper
{
    public static Db loadDb( String dbClassName ) throws DbException
    {
        try
        {
            Class<? extends Db> dbClass = loadClass( dbClassName, Db.class );
            return dbClass.getConstructor().newInstance();
        }
        catch ( Exception e )
        {
            throw new DbException(
                    String.format( "Error creating DB [%s] from dynamically loaded class", dbClassName ), e.getCause() );
        }
    }

    public static Workload loadWorkload( String workloadClassName ) throws WorkloadException
    {
        try
        {
            Class<? extends Workload> workloadClass = loadClass( workloadClassName, Workload.class );
            return workloadClass.getConstructor().newInstance();
        }
        catch ( Exception e )
        {
            throw new WorkloadException( "Error creating Workload from dynamically loaded class", e.getCause() );
        }
    }

    public static MeasurementsExporter loadMeasurementsExporter( String measurementsExporterClassName, OutputStream out )
            throws MeasurementsException
    {
        try
        {
            Class<? extends MeasurementsExporter> measurementsExporterClass = loadClass( measurementsExporterClassName,
                    MeasurementsExporter.class );
            return measurementsExporterClass.getConstructor( OutputStream.class ).newInstance( out );
        }
        catch ( Exception e )
        {
            throw new MeasurementsException( "Error creating MeasurementsExporter from dynamically loaded class",
                    e.getCause() );
        }
    }

    private static <C> Class<? extends C> loadClass( String className, Class<C> baseClass )
            throws ClassNotFoundException
    {
        ClassLoader classLoader = ClassLoaderHelper.class.getClassLoader();
        Class<? extends C> loadedClass = (Class<? extends C>) classLoader.loadClass( className );
        return loadedClass;
    }

}
