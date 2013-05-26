package com.ldbc.driver.workloads;

import java.util.Map;

import com.ldbc.driver.Client;
import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorBuilder;
import com.ldbc.driver.util.MapUtils;

public abstract class Workload
{
    private boolean isInitialized = false;
    private boolean isCleanedUp = false;

    private long insertStart;
    private long recordCount;

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
        recordCount = Long.parseLong( properties.get( Client.RECORD_COUNT_ARG ) );
        insertStart = Long.parseLong( MapUtils.mapGetDefault( properties, Client.INSERT_START_ARG,
                Client.INSERT_START_DEFAULT ) );
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

    protected final long getInsertStart()
    {
        return insertStart;
    }

    protected final long getRecordCount()
    {
        return recordCount;
    }
}
