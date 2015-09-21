package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;

import java.util.Iterator;

// TODO test
class InitiatedTimeSubmittingOperationRetriever
{
    private final Iterator<Operation> nonDependencyOperations;
    private final Iterator<Operation> dependencyOperations;
    private final LocalCompletionTimeWriter localCompletionTimeWriter;
    private Operation nextNonDependencyOperation = null;
    private Operation nextDependencyOperation = null;

    InitiatedTimeSubmittingOperationRetriever( WorkloadStreams.WorkloadStreamDefinition streamDefinition,
            LocalCompletionTimeWriter localCompletionTimeWriter )
    {
        this.nonDependencyOperations = streamDefinition.nonDependencyOperations();
        this.dependencyOperations = streamDefinition.dependencyOperations();
        this.localCompletionTimeWriter = localCompletionTimeWriter;
    }

    public boolean hasNextOperation()
    {
        return nonDependencyOperations.hasNext() || dependencyOperations.hasNext();
    }

    /*
    1. get next operation (both dependent & dependency)
    2. submit initiated time
    4. return operation with lowest scheduled start time
     */
    public Operation nextOperation() throws OperationExecutorException, CompletionTimeException
    {
        if ( dependencyOperations.hasNext() && null == nextDependencyOperation )
        {
            nextDependencyOperation = dependencyOperations.next();
            // submit initiated time as soon as possible so GCT/dependencies can advance as soon as possible
            localCompletionTimeWriter.submitLocalInitiatedTime( nextDependencyOperation.timeStamp() );
            if ( false == dependencyOperations.hasNext() )
            {
                // after last write operation, submit highest possible initiated time to ensure that GCT progresses
                // to time of highest LCT write
                localCompletionTimeWriter.submitLocalInitiatedTime( Long.MAX_VALUE );
            }
        }
        if ( nonDependencyOperations.hasNext() && null == nextNonDependencyOperation )
        {
            nextNonDependencyOperation = nonDependencyOperations.next();
            // no need to submit initiated time for an operation that should not write to GCT
        }
        // return operation with lowest start time
        if ( null != nextDependencyOperation && null != nextNonDependencyOperation )
        {
            Operation nextOperation;
            if ( nextNonDependencyOperation.timeStamp() < nextDependencyOperation.timeStamp() )
            {
                nextOperation = nextNonDependencyOperation;
                nextNonDependencyOperation = null;
            }
            else
            {
                nextOperation = nextDependencyOperation;
                nextDependencyOperation = null;
            }
            return nextOperation;
        }
        else if ( null == nextDependencyOperation && null != nextNonDependencyOperation )
        {
            Operation nextOperation = nextNonDependencyOperation;
            nextNonDependencyOperation = null;
            return nextOperation;
        }
        else if ( null != nextDependencyOperation && null == nextNonDependencyOperation )
        {
            Operation nextOperation = nextDependencyOperation;
            nextDependencyOperation = null;
            return nextOperation;
        }
        else
        {
            throw new OperationExecutorException( "Unexpected error in " + getClass().getSimpleName() );
        }
    }
}
