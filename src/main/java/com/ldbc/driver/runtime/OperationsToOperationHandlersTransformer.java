package com.ldbc.driver.runtime;

import com.ldbc.driver.*;
import com.ldbc.driver.runtime.coordination.*;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.GctDependencyCheck;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TimeSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// TODO test
class OperationsToOperationHandlersTransformer {
    private final TimeSource TIME_SOURCE;
    private final Db db;
    private final Spinner spinner;
    private final ConcurrentCompletionTimeService completionTimeService;
    private final ConcurrentErrorReporter errorReporter;
    private final ConcurrentMetricsService metricsService;
    private final Map<Class<? extends Operation>, OperationClassification> operationClassifications;
    private LocalCompletionTimeWriter dummyLocalCompletionTimeWriter = new DummyLocalCompletionTimeWriter();
    private LocalCompletionTimeWriter blockingLocalCompletionTimeWriter;
    private LocalCompletionTimeWriter asynchronousLocalCompletionTimeWriter;
    private LocalCompletionTimeWriter windowedLocalCompletionTimeWriter;
    private GlobalCompletionTimeReader globalCompletionTimeReader;

    OperationsToOperationHandlersTransformer(TimeSource timeSource,
                                             Db db,
                                             Spinner spinner,
                                             ConcurrentCompletionTimeService completionTimeService,
                                             ConcurrentErrorReporter errorReporter,
                                             ConcurrentMetricsService metricsService,
                                             Map<Class<? extends Operation>, OperationClassification> operationClassifications) {
        this.TIME_SOURCE = timeSource;
        this.db = db;
        this.spinner = spinner;
        this.completionTimeService = completionTimeService;
        this.errorReporter = errorReporter;
        this.metricsService = metricsService;
        this.operationClassifications = operationClassifications;
        this.blockingLocalCompletionTimeWriter = null;
        this.asynchronousLocalCompletionTimeWriter = null;
        this.windowedLocalCompletionTimeWriter = null;
        this.globalCompletionTimeReader = completionTimeService;
    }

    private LocalCompletionTimeWriter getOrCreateBlockingLocalCompletionTimeWriter() throws CompletionTimeException {
        if (null == blockingLocalCompletionTimeWriter) {
            blockingLocalCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();
        }
        return blockingLocalCompletionTimeWriter;
    }

    private LocalCompletionTimeWriter getOrCreateAsynchronousLocalCompletionTimeWriter() throws CompletionTimeException {
        if (null == asynchronousLocalCompletionTimeWriter) {
            asynchronousLocalCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();
        }
        return asynchronousLocalCompletionTimeWriter;
    }

    private LocalCompletionTimeWriter getOrCreateWindowedLocalCompletionTimeWriter() throws CompletionTimeException {
        if (null == windowedLocalCompletionTimeWriter) {
            windowedLocalCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();
        }
        return windowedLocalCompletionTimeWriter;
    }

    List<OperationHandler<?>> transform(List<Operation<?>> operations) throws WorkloadException {
        List<OperationHandler<?>> operationHandlers = new ArrayList<>();

        // create one writer for every scheduling mode that appears in the operation stream
        for (Operation<?> operation : operations) {
            if (blockingLocalCompletionTimeWriter != null && asynchronousLocalCompletionTimeWriter != null && windowedLocalCompletionTimeWriter != null) {
                // all writers have been created
                break;
            }
            OperationClassification.GctMode operationGctMode = operationClassifications.get(operation.getClass()).gctMode();
            OperationClassification.SchedulingMode operationSchedulingMode = operationClassifications.get(operation.getClass()).schedulingMode();

            try {
                if (operationGctMode.equals(OperationClassification.GctMode.READ_WRITE)) {
                    switch (operationSchedulingMode) {
                        case INDIVIDUAL_ASYNC:
                            getOrCreateAsynchronousLocalCompletionTimeWriter();
                            break;
                        case INDIVIDUAL_BLOCKING:
                            getOrCreateBlockingLocalCompletionTimeWriter();
                            break;
                        case WINDOWED:
                            getOrCreateWindowedLocalCompletionTimeWriter();
                            break;
                        default:
                            throw new WorkloadException(String.format("Unrecognized Scheduling Mode: %s", operationClassifications.get(operation.getClass()).gctMode()));
                    }
                }
            } catch (CompletionTimeException e) {
                throw new WorkloadException("Error while trying to create local completion time writer", e);
            }
        }

        boolean atLeastOneLocalCompletionWriterHasBeenCreated =
                asynchronousLocalCompletionTimeWriter != null || blockingLocalCompletionTimeWriter != null || windowedLocalCompletionTimeWriter != null;

        for (Operation<?> operation : operations) {
            OperationHandler<?> operationHandler;
            try {
                operationHandler = db.getOperationHandler(operation);
            } catch (DbException e) {
                throw new WorkloadException(
                        String.format("Error while trying to retrieve operation handler for operation\n%s", operation),
                        e);
            }
            OperationClassification.GctMode operationGctMode = operationClassifications.get(operation.getClass()).gctMode();
            OperationClassification.SchedulingMode operationSchedulingMode = operationClassifications.get(operation.getClass()).schedulingMode();

            LocalCompletionTimeWriter localCompletionTimeWriter;

            try {
                if (operationGctMode.equals(OperationClassification.GctMode.READ_WRITE)) {
                    switch (operationSchedulingMode) {
                        case INDIVIDUAL_ASYNC:
                            localCompletionTimeWriter = getOrCreateAsynchronousLocalCompletionTimeWriter();
                            break;
                        case INDIVIDUAL_BLOCKING:
                            localCompletionTimeWriter = getOrCreateBlockingLocalCompletionTimeWriter();
                            break;
                        case WINDOWED:
                            localCompletionTimeWriter = getOrCreateWindowedLocalCompletionTimeWriter();
                            break;
                        default:
                            throw new WorkloadException(String.format("Unrecognized Scheduling Mode: %s", operationClassifications.get(operation.getClass()).gctMode()));
                    }
                } else {
                    localCompletionTimeWriter = dummyLocalCompletionTimeWriter;
                }
            } catch (CompletionTimeException e) {
                throw new WorkloadException("Error while trying to create local completion time writer", e);
            }

            try {
                switch (operationGctMode) {
                    case READ_WRITE:
                        operationHandler.init(TIME_SOURCE, spinner, operation, localCompletionTimeWriter, errorReporter, metricsService);
                        if (atLeastOneLocalCompletionWriterHasBeenCreated) {
                            operationHandler.addCheck(new GctDependencyCheck(globalCompletionTimeReader, operation, errorReporter));
                        }
                        break;
                    case READ:
                        operationHandler.init(TIME_SOURCE, spinner, operation, localCompletionTimeWriter, errorReporter, metricsService);
                        if (atLeastOneLocalCompletionWriterHasBeenCreated) {
                            operationHandler.addCheck(new GctDependencyCheck(globalCompletionTimeReader, operation, errorReporter));
                        }
                        break;
                    case NONE:
                        operationHandler.init(TIME_SOURCE, spinner, operation, localCompletionTimeWriter, errorReporter, metricsService);
                        break;
                    default:
                        throw new WorkloadException(
                                String.format("Unrecognized GctMode: %s", operationClassifications.get(operation.getClass()).gctMode()));
                }
            } catch (OperationException e) {
                throw new WorkloadException(String.format("Error while trying to initialize operation handler\n%s", operationHandler), e);
            }

            operationHandlers.add(operationHandler);
        }

        return operationHandlers;
    }
}