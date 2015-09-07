package com.ldbc.driver.workloads;

import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.util.ClassLoaderHelper;

import static java.lang.String.format;

public class ClassNameWorkloadFactory implements WorkloadFactory
{
    private final String workloadClassName;

    public ClassNameWorkloadFactory( String workloadClassName )
    {
        this.workloadClassName = workloadClassName;
    }

    public Workload createWorkload() throws WorkloadException
    {
        try
        {
            return ClassLoaderHelper.loadWorkload( workloadClassName );
        }
        catch ( Exception e )
        {
            throw new WorkloadException( format( "Error loading Workload class: %s", workloadClassName ), e );
        }
    }
}
