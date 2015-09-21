package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.ChildOperationGenerator;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandlerRunnableContext;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;

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
