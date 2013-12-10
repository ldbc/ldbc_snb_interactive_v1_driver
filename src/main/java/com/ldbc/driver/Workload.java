package com.ldbc.driver;

import java.util.Iterator;
import java.util.Map;

import com.ldbc.driver.generator.GeneratorFactory;

public abstract class Workload
{
    private boolean isInitialized = false;
    private boolean isCleanedUp = false;

    private long operationCount;
    private long recordCount;

    /**
     * Called once to initialize state for workload
     */
    public final void init( WorkloadParams params ) throws WorkloadException
    {
        if ( true == isInitialized )
        {
            throw new WorkloadException( "DB may be initialized only once" );
        }
        isInitialized = true;
        this.operationCount = params.getOperationCount();
        this.recordCount = params.getRecordCount();
        onInit( params.asMap() );
    }

    protected long getOperationCount()
    {
        return operationCount;
    }

    protected long getRecordCount()
    {
        return recordCount;
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

    public final Iterator<Operation<?>> getLoadOperations( GeneratorFactory generators ) throws WorkloadException
    {
        if ( WorkloadParams.UNBOUNDED_OPERATION_COUNT == getOperationCount() )
        {
            return createLoadOperations( generators );
        }
        else
        {
            return generators.limit( createLoadOperations( generators ), getOperationCount() );
        }
    }

    protected abstract Iterator<Operation<?>> createLoadOperations( GeneratorFactory generators )
            throws WorkloadException;

    public final Iterator<Operation<?>> getTransactionalOperations( GeneratorFactory generators )
            throws WorkloadException
    {
        if ( -1 == getOperationCount() )
        {
            return createTransactionalOperations( generators );
        }
        else
        {
            return generators.limit( createTransactionalOperations( generators ), getOperationCount() );
        }
    }

    protected abstract Iterator<Operation<?>> createTransactionalOperations( GeneratorFactory generators )
            throws WorkloadException;
}
