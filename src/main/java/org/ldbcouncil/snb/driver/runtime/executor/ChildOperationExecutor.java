package org.ldbcouncil.snb.driver.runtime.executor;

import org.ldbcouncil.snb.driver.ChildOperationGenerator;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.OperationHandlerRunnableContext;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.runtime.coordination.CompletionTimeException;

public class ChildOperationExecutor
{
    public void execute(
            ChildOperationGenerator childOperationGenerator,
            Operation operation,
            Object result,
            long actualStartTimeAsMilli,
            long runDurationAsNano,
            OperationHandlerRunnableContextRetriever operationHandlerRunnableContextRetriever )
            throws WorkloadException, DbException, OperationExecutorException, CompletionTimeException
    {
        if ( null == childOperationGenerator )
        {
            return;
        }
        else
        {
            if ( null != childOperationGenerator )
            {
                double state = childOperationGenerator.initialState();
                operation = childOperationGenerator.nextOperation(
                        state,
                        operation,
                        result,
                        actualStartTimeAsMilli,
                        runDurationAsNano
                );
                while ( null != operation )
                {
                    OperationHandlerRunnableContext childOperationHandlerRunnableContext =
                            operationHandlerRunnableContextRetriever.getInitializedHandlerFor( operation );
                    childOperationHandlerRunnableContext.run();
                    state = childOperationGenerator.updateState( state, operation.type() );
                    operation = childOperationGenerator.nextOperation(
                            state,
                            childOperationHandlerRunnableContext.operation(),
                            childOperationHandlerRunnableContext.resultReporter().result(),
                            childOperationHandlerRunnableContext.resultReporter().actualStartTimeAsMilli(),
                            childOperationHandlerRunnableContext.resultReporter().runDurationAsNano()
                    );
                    childOperationHandlerRunnableContext.cleanup();
                }
            }
        }
    }
}
