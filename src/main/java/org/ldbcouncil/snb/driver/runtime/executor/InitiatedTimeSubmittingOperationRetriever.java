package org.ldbcouncil.snb.driver.runtime.executor;

import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.WorkloadStreams;
import org.ldbcouncil.snb.driver.runtime.coordination.CompletionTimeException;
import org.ldbcouncil.snb.driver.runtime.coordination.CompletionTimeReader;
import org.ldbcouncil.snb.driver.runtime.coordination.CompletionTimeWriter;

import java.util.Iterator;

class InitiatedTimeSubmittingOperationRetriever
{
    private final Iterator<Operation> nonDependencyOperations;
    private final Iterator<Operation> dependencyOperations;
    private final CompletionTimeWriter completionTimeWriter;
    private final CompletionTimeReader completionTimeReader;
    private Operation nextNonDependencyOperation = null;
    private Operation nextDependencyOperation = null;
    private boolean isEnabledDependencyOperations = false;

    InitiatedTimeSubmittingOperationRetriever(
        WorkloadStreams.WorkloadStreamDefinition streamDefinition,
        CompletionTimeWriter completionTimeWriter,
        CompletionTimeReader completionTimeReader
    )
    {
        this.nonDependencyOperations = streamDefinition.nonDependencyOperations();
        this.dependencyOperations = streamDefinition.dependencyOperations();
        this.isEnabledDependencyOperations = this.dependencyOperations.hasNext();
        this.completionTimeWriter = completionTimeWriter;
        this.completionTimeReader = completionTimeReader;
    }

    boolean hasNextOperation()
    {
        return nonDependencyOperations.hasNext() || dependencyOperations.hasNext();
    }

    /*
    1. get next operation (both dependent & dependency)
    2. submit initiated time
    4. return operation with lowest scheduled start time
     */
    Operation nextOperation() throws OperationExecutorException, CompletionTimeException
    {
        if ( dependencyOperations.hasNext() && null == nextDependencyOperation )
        {
            nextDependencyOperation = dependencyOperations.next();
            // submit initiated time as soon as possible so /dependencies can advance as soon as possible
            completionTimeWriter.submitInitiatedTime( nextDependencyOperation.timeStamp() );
        }
        // Non-dependency operations are operations that are not a dependency to other operations,
        // but can be dependent on others. Therefore, only read queries are considered here.
        if ( nonDependencyOperations.hasNext() && null == nextNonDependencyOperation )
        {

            nextNonDependencyOperation = nonDependencyOperations.next();

            if (isEnabledDependencyOperations)
            {
                long currentCompletionTime = completionTimeReader.completionTimeAsMilli();

                while (nonDependencyOperations.hasNext() && currentCompletionTime > 0)
                {
                    if (nextNonDependencyOperation.dependencyTimeStamp() < currentCompletionTime && nextNonDependencyOperation.expiryTimeStamp() > currentCompletionTime)
                    {
                        break;
                    }
                    nextNonDependencyOperation = nonDependencyOperations.next();
                }
            }
            // else do nothing and just use the operation since the completion time will not update

            // no need to submit initiated time for an operation that should not write to CT
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
