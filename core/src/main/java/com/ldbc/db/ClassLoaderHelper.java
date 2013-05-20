package com.ldbc.db;

import com.ldbc.workloads.Workload;
import com.ldbc.workloads.WorkloadException;

public class ClassLoaderHelper
{
    public Db loadDbInstance( String dbClassName ) throws DbException
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

    public Workload loadWorkloadInstance( String workloadClassName ) throws WorkloadException
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

    private <C> Class<? extends C> loadClass( String className, Class<C> baseClass ) throws ClassNotFoundException
    {
        ClassLoader classLoader = ClassLoaderHelper.class.getClassLoader();
        Class<? extends C> loadedClass = (Class<? extends C>) classLoader.loadClass( className );
        return loadedClass;
    }

}
