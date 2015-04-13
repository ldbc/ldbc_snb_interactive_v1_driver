package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.*;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.DummyLocalCompletionTimeWriter;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.MetricsService;
import com.ldbc.driver.runtime.scheduling.GctDependencyCheck;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TimeSource;

import java.util.Set;

// TODO test
class OperationHandlerRunnableContextRetriever {
    private static final LocalCompletionTimeWriter DUMMY_LOCAL_COMPLETION_TIME_WRITER = new DummyLocalCompletionTimeWriter();
    private final Db db;
    private final LocalCompletionTimeWriter localCompletionTimeWriter;
    private final GlobalCompletionTimeReader globalCompletionTimeReader;
    private final Spinner spinner;
    private final TimeSource timeSource;
    private final ConcurrentErrorReporter errorReporter;
    private final MetricsService metricsService;
    private final Set<Class<? extends Operation>> dependencyOperationTypes;
    private final Set<Class<? extends Operation>> dependentOperationTypes;

    OperationHandlerRunnableContextRetriever(WorkloadStreams.WorkloadStreamDefinition streamDefinition,
                                             Db db,
                                             LocalCompletionTimeWriter localCompletionTimeWriter,
                                             GlobalCompletionTimeReader globalCompletionTimeReader,
                                             Spinner spinner,
                                             TimeSource timeSource,
                                             ConcurrentErrorReporter errorReporter,
                                             MetricsService metricsService) {
        this.db = db;
        this.localCompletionTimeWriter = localCompletionTimeWriter;
        this.globalCompletionTimeReader = globalCompletionTimeReader;
        this.spinner = spinner;
        this.timeSource = timeSource;
        this.errorReporter = errorReporter;
        this.metricsService = metricsService;
        this.dependentOperationTypes = streamDefinition.dependentOperationTypes();
        this.dependencyOperationTypes = streamDefinition.dependencyOperationTypes();
    }

    public OperationHandlerRunnableContext getInitializedHandlerFor(Operation operation) throws OperationExecutorException, CompletionTimeException, DbException {
        OperationHandlerRunnableContext operationHandlerRunnableContext;
        try {
            operationHandlerRunnableContext = db.getOperationHandlerRunnableContext(operation);
        } catch (Exception e) {
            throw new OperationExecutorException(String.format("Error while retrieving handler for operation\nOperation: %s", operation), e);
        }
        LocalCompletionTimeWriter localCompletionTimeWriterForHandler;
        if (dependencyOperationTypes.contains(operation.getClass())) {
            localCompletionTimeWriterForHandler = localCompletionTimeWriter;
        } else {
            localCompletionTimeWriterForHandler = DUMMY_LOCAL_COMPLETION_TIME_WRITER;
        }
        try {
            operationHandlerRunnableContext.init(timeSource, spinner, operation, localCompletionTimeWriterForHandler, errorReporter, metricsService);
        } catch (Exception e) {
            throw new OperationExecutorException(String.format("Error while initializing handler for operation\nOperation: %s", operation), e);
        }
        if (dependentOperationTypes.contains(operation.getClass())) {
            operationHandlerRunnableContext.setBeforeExecuteCheck(new GctDependencyCheck(globalCompletionTimeReader, operation, errorReporter));
        }
        return operationHandlerRunnableContext;
    }
}
