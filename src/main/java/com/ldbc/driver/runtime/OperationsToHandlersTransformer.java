package com.ldbc.driver.runtime;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.*;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.coordination.ReadOnlyConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.GctCheck;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;

import java.util.Iterator;
import java.util.Map;

// TODO test
class OperationsToHandlersTransformer {
    private final Db db;
    private final Spinner spinner;
    private final ConcurrentCompletionTimeService completionTimeService;
    private final ConcurrentErrorReporter errorReporter;
    private final ConcurrentMetricsService metricsService;
    private final Duration gctDeltaDuration;
    private final Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications;

    OperationsToHandlersTransformer(Db db,
                                    Spinner spinner,
                                    ConcurrentCompletionTimeService completionTimeService,
                                    ConcurrentErrorReporter errorReporter,
                                    ConcurrentMetricsService metricsService,
                                    Duration gctDeltaDuration,
                                    Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications) {
        this.db = db;
        this.spinner = spinner;
        this.completionTimeService = completionTimeService;
        this.errorReporter = errorReporter;
        this.metricsService = metricsService;
        this.gctDeltaDuration = gctDeltaDuration;
        this.operationClassifications = operationClassifications;
    }

    Iterator<OperationHandler<?>> transform(Iterator<Operation<?>> operations) throws WorkloadException {
        try {
            return Iterators.transform(operations, new Function<Operation<?>, OperationHandler<?>>() {
                @Override
                public OperationHandler<?> apply(Operation<?> operation) {
                    try {
                        OperationHandler<?> operationHandler = db.getOperationHandler(operation);
                        switch (operationClassifications.get(operation.getClass()).gctMode()) {
                            case READ_WRITE:
                                operationHandler.init(spinner, operation, completionTimeService, errorReporter, metricsService);
                                operationHandler.addCheck(new GctCheck(completionTimeService, gctDeltaDuration, operation, errorReporter));
                                break;
                            case READ:
                                operationHandler.init(spinner, operation, new ReadOnlyConcurrentCompletionTimeService(completionTimeService), errorReporter, metricsService);
                                operationHandler.addCheck(new GctCheck(completionTimeService, gctDeltaDuration, operation, errorReporter));
                                break;
                            case NONE:
                                operationHandler.init(spinner, operation, new ReadOnlyConcurrentCompletionTimeService(completionTimeService), errorReporter, metricsService);
                                break;
                            default:
                                throw new WorkloadException(String.format("Unrecognized GctMode: %s", operationClassifications.get(operation.getClass()).gctMode()));
                        }
                        return operationHandler;
                    } catch (Exception e) {
                        // errorReporter.reportError(this, String.format("Unexpected error in operationsToHandlers()\n%s", ConcurrentErrorReporter.stackTraceToString(e)));
                        throw new RuntimeException("Unexpected error in operationsToHandlers()", e);
                    }
                }
            });
        } catch (Throwable e) {
            throw new WorkloadException("Error encountered while transforming Operation stream to OperationHandler stream", e);
        }
    }
}
