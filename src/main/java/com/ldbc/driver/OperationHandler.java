package com.ldbc.driver;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.scheduling.MultiCheck;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.runtime.scheduling.SpinnerCheck;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.Function0;
import stormpot.Poolable;
import stormpot.Slot;

import java.util.ArrayList;
import java.util.List;

public abstract class OperationHandler<OPERATION_TYPE extends Operation<?>> implements Runnable, Poolable {
    private Slot slot;
    private TimeSource timeSource;
    private Spinner spinner;
    private OPERATION_TYPE operation;
    private DbConnectionState dbConnectionState;
    private LocalCompletionTimeWriter localCompletionTimeWriter;
    private ConcurrentErrorReporter errorReporter;
    private ConcurrentMetricsService metricsService;
    private List<SpinnerCheck> beforeExecuteChecks;
    private List<Function0> onCompleteTasks;

    private boolean initialized = false;

    public final void setSlot(Slot slot) {
        this.slot = slot;
    }

    public final void init(TimeSource timeSource,
                           Spinner spinner,
                           Operation<?> operation,
                           LocalCompletionTimeWriter localCompletionTimeWriter,
                           ConcurrentErrorReporter errorReporter,
                           ConcurrentMetricsService metricsService) throws OperationException {
        if (initialized) {
            throw new OperationException(String.format("OperationHandler can not be initialized twice\n%s", toString()));
        }
        this.timeSource = timeSource;
        this.spinner = spinner;
        this.operation = (OPERATION_TYPE) operation;
        this.localCompletionTimeWriter = localCompletionTimeWriter;
        this.errorReporter = errorReporter;
        this.metricsService = metricsService;
        this.beforeExecuteChecks = new ArrayList<>();
        this.onCompleteTasks = new ArrayList<>();

        this.initialized = true;
    }

    public final OPERATION_TYPE operation() {
        return operation;
    }

    public final LocalCompletionTimeWriter localCompletionTimeWriter() {
        return localCompletionTimeWriter;
    }

    public final void setDbConnectionState(DbConnectionState dbConnectionState) {
        this.dbConnectionState = dbConnectionState;
    }

    public final DbConnectionState dbConnectionState() {
        return dbConnectionState;
    }

    public final void addBeforeExecuteCheck(SpinnerCheck check) {
        beforeExecuteChecks.add(check);
    }

    public final void addOnCompleteTask(Function0 task) {
        onCompleteTasks.add(task);
    }

    /**
     * Internally calls the protected method executeOperation(operation)
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
            if (false == spinner.waitForScheduledStartTime(operation, new MultiCheck(beforeExecuteChecks))) {
                // TODO something more elaborate here? see comments in Spinner
                // Spinner result indicates operation should not be processed
                return;
            }
            long actualStartTimeAsMilli = timeSource.nowAsMilli();
            long startOfLatencyMeasurementAsNano = timeSource.nanoSnapshot();
            OperationResultReport operationResultReport = executeOperation(operation);
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

    /**
     * DO NOT call this method for regular workload execution. It is only intended for validation purposes.
     *
     * @param operation
     * @return operation result
     * @throws DbException
     */
    public final OperationResultReport executeOperationUnsafe(OPERATION_TYPE operation) throws DbException {
        return executeOperation(operation);
    }

    protected abstract OperationResultReport executeOperation(OPERATION_TYPE operation) throws DbException;

    @Override
    public String toString() {
        return String.format("OperationHandler [type=%s, operation=%s]", getClass().getName(), operation);
    }

    public final void onComplete() {
        for (int i = 0; i < onCompleteTasks.size(); i++) {
            onCompleteTasks.get(i).apply();
        }
    }

    public final void cleanup() {
        release();
    }

    // Note, this should not really be public API, it is from the StormPot Poolable interface
    @Override
    public final void release() {
        initialized = false;
        timeSource = null;
        spinner = null;
        operation = null;
        dbConnectionState = null;
        localCompletionTimeWriter = null;
        errorReporter = null;
        metricsService = null;
        beforeExecuteChecks = null;
        if (null != slot)
            slot.release(this);
    }
}
