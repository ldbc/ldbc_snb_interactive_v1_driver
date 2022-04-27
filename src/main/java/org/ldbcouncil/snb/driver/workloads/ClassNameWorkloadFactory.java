package org.ldbcouncil.snb.driver.workloads;

import org.ldbcouncil.snb.driver.Workload;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.util.ClassLoaderHelper;

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
