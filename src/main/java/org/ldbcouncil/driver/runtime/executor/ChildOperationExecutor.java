package org.ldbcouncil.driver.runtime.executor;

import org.ldbcouncil.driver.ChildOperationGenerator;
import org.ldbcouncil.driver.DbException;
import org.ldbcouncil.driver.Operation;
import org.ldbcouncil.driver.OperationHandlerRunnableContext;
import org.ldbcouncil.driver.WorkloadException;
import org.ldbcouncil.driver.runtime.coordination.CompletionTimeException;

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
