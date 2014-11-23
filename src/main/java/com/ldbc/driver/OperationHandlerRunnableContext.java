package com.ldbc.driver;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.runtime.scheduling.SpinnerCheck;
import com.ldbc.driver.temporal.TimeSource;
import stormpot.Poolable;
import stormpot.Slot;

public class OperationHandlerRunnableContext implements Runnable, Poolable {
    private Slot slot = null;
    private TimeSource timeSource = null;
    private Spinner spinner = null;
    private Operation operation = null;
    private DbConnectionState dbConnectionState = null;
    private LocalCompletionTimeWriter localCompletionTimeWriter = null;
    private ConcurrentErrorReporter errorReporter = null;
    private ConcurrentMetricsService metricsService = null;
    private SpinnerCheck beforeExecuteCheck = Spinner.TRUE_CHECK;
    private OperationHandler operationHandler = null;
    private boolean initialized = false;

    public final void setSlot(Slot slot) {
        this.slot = slot;
    }

    public final void init(TimeSource timeSource,
                           Spinner spinner,
                           Operation operation,
                           LocalCompletionTimeWriter localCompletionTimeWriter,
                           ConcurrentErrorReporter errorReporter,
                           ConcurrentMetricsService metricsService) throws OperationException {
        if (initialized) {
            throw new OperationException(String.format("%s can not be initialized twice", getClass().getSimpleName()));
        }
        this.timeSource = timeSource;
        this.spinner = spinner;
        this.operation = operation;
        this.localCompletionTimeWriter = localCompletionTimeWriter;
        this.errorReporter = errorReporter;
        this.metricsService = metricsService;

        this.initialized = true;
    }

    public final void setOperationHandler(OperationHandler operationHandler) {
        this.operationHandler = operationHandler;
    }

    public final void setDbConnectionState(DbConnectionState dbConnectionState) {
        this.dbConnectionState = dbConnectionState;
    }

    public final void setBeforeExecuteCheck(SpinnerCheck check) {
        beforeExecuteCheck = check;
    }

    public final Operation operation() {
        return operation;
    }

    public final OperationHandler operationHandler() {
        return operationHandler;
    }

    public final LocalCompletionTimeWriter localCompletionTimeWriter() {
        return localCompletionTimeWriter;
    }

    public final DbConnectionState dbConnectionState() {
        return dbConnectionState;
    }

    /**
     * Internally calls the method executeOperation(operation)
     * and returns the associated OperationResultReport if execution was successful.
     * If execution is successful OperationResultReport metrics are also written to ConcurrentMetricsService.
     * If execution is unsuccessful the result is null, an error is written to ConcurrentErrorReporter,
     * and no metrics are written.
     *
     * @return an OperationResultReport if Operation execution was successful, otherwise null
     */
    @Override
    public void run() {
        if (false == initialized) {
            errorReporter.reportError(this, "Handler was executed before being initialized");
            return;
        }
        try {
            if (false == spinner.waitForScheduledStartTime(operation, beforeExecuteCheck)) {
                // TODO something more elaborate here? see comments in Spinner
                // TODO should probably report failed operation
                // Spinner result indicates operation should not be processed
                return;
            }
            long actualStartTimeAsMilli = timeSource.nowAsMilli();
            long startOfLatencyMeasurementAsNano = timeSource.nanoSnapshot();
            OperationResultReport operationResultReport = operationHandler.executeOperation(operation, dbConnectionState);
            long endOfLatencyMeasurementAsNano = timeSource.nanoSnapshot();
            if (null == operationResultReport) {
                throw new DbException(String.format("Handler returned null result:\n %s", toString()));
            }
            operationResultReport.setRunDurationAsNano(endOfLatencyMeasurementAsNano - startOfLatencyMeasurementAsNano);
            operationResultReport.setActualStartTimeAsMilli(actualStartTimeAsMilli);
            localCompletionTimeWriter.submitLocalCompletedTime(operation.timeStamp());
            metricsService.submitOperationResult(operationResultReport);
        } catch (DbException e) {
            String errMsg = String.format(
                    "Error encountered while executing query %s\n%s",
                    operation.toString(),
                    ConcurrentErrorReporter.stackTraceToString(e));
            errorReporter.reportError(this, errMsg);
        } catch (MetricsCollectionException e) {
            String errMsg = String.format(
                    "Error encountered while collecting metrics for query %s\n%s",
                    operation.toString(),
                    ConcurrentErrorReporter.stackTraceToString(e));
            errorReporter.reportError(this, errMsg);
        } catch (CompletionTimeException e) {
            String errMsg = String.format(
                    "Error encountered while submitting completed time for query %s\n%s",
                    operation.toString(),
                    ConcurrentErrorReporter.stackTraceToString(e));
            errorReporter.reportError(this, errMsg);
        } catch (Throwable e) {
            String errMsg = String.format(
                    "Unexpected error while executing query %s\n%s",
                    operation.toString(),
                    ConcurrentErrorReporter.stackTraceToString(e));
            errorReporter.reportError(this, errMsg);
        }
    }


    @Override
    public String toString() {
        return "OperationHandlerRunner{" +
                "slot=" + slot +
                ", timeSource=" + timeSource +
                ", spinner=" + spinner +
                ", operation=" + operation +
                ", dbConnectionState=" + dbConnectionState +
                ", localCompletionTimeWriter=" + localCompletionTimeWriter +
                ", errorReporter=" + errorReporter +
                ", metricsService=" + metricsService +
                ", beforeExecuteCheck=" + beforeExecuteCheck +
                ", operationHandler=" + operationHandler +
                ", initialized=" + initialized +
                '}';
    }

    public final void cleanup() {
        release();
    }

    // Note, this should not really be public API, it is from the StormPot Poolable interface
    @Override
    public final void release() {
        initialized = false;
        if (null != slot)
            slot.release(this);
    }
}
