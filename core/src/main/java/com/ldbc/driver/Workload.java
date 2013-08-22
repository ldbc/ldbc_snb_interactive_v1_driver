package com.ldbc.driver;

import java.util.Map;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorBuilder;
import com.ldbc.driver.generator.wrapper.CappedGeneratorWrapper;

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

    public final Generator<Operation<?>> getLoadOperations( GeneratorBuilder generatorBuilder )
            throws WorkloadException
    {
        if ( WorkloadParams.UNBOUNDED_OPERATION_COUNT == getOperationCount() )
        {
            return createLoadOperations( generatorBuilder );
        }
        else
        {
            return new CappedGeneratorWrapper<Operation<?>>( createLoadOperations( generatorBuilder ),
                    getOperationCount() );
        }
    }

    protected abstract Generator<Operation<?>> createLoadOperations( GeneratorBuilder generatorBuilder )
            throws WorkloadException;

    public final Generator<Operation<?>> getTransactionalOperations( GeneratorBuilder generatorBuilder )
            throws WorkloadException
    {
        if ( -1 == getOperationCount() )
        {
            return createTransactionalOperations( generatorBuilder );
        }
        else
        {
            return new CappedGeneratorWrapper<Operation<?>>( createTransactionalOperations( generatorBuilder ),
                    getOperationCount() );
        }
    }

    protected abstract Generator<Operation<?>> createTransactionalOperations( GeneratorBuilder generatorBuilder )
            throws WorkloadException;
}
