package com.ldbc.workloads;

import java.util.Map;

import com.ldbc.Operation;
import com.ldbc.generator.Generator;
import com.ldbc.generator.GeneratorBuilder;

public abstract class Workload
{
    private boolean isInitialized = false;
    private boolean isCleanedUp = false;

    /**
     * Called once to initialize state for workload
     */
    public final void init( Map<String, String> properties ) throws WorkloadException
    {
        if ( true == isInitialized )
        {
            throw new WorkloadException( "DB may be initialized only once" );
        }
        isInitialized = true;
        onInit( properties );
    }

    public abstract void onInit( Map<String, String> properties ) throws WorkloadException;

    public final void cleanup() throws WorkloadException
    {
        if ( true == isCleanedUp )
        {
            throw new WorkloadException( "Workload may be cleaned up only once" );
        }
        isCleanedUp = true;
        onCleanup();
    }

    protected abstract void onCleanup() throws WorkloadException;

    public abstract Generator<Operation<?>> getLoadOperations( GeneratorBuilder generatorBuilder )
            throws WorkloadException;

    public abstract Generator<Operation<?>> getTransactionalOperations( GeneratorBuilder generatorBuilder )
            throws WorkloadException;
}
