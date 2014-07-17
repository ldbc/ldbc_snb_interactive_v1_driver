package com.ldbc.driver;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.scheduling.MultiCheck;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.runtime.scheduling.SpinnerCheck;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public abstract class OperationHandler<OPERATION_TYPE extends Operation<?>> implements Callable<OperationResultReport> {
    private TimeSource TIME_SOURCE;
    private Spinner spinner;
    private OPERATION_TYPE operation;
    private DbConnectionState dbConnectionState;
    private ConcurrentCompletionTimeService completionTimeService;
    private ConcurrentErrorReporter errorReporter;
    private ConcurrentMetricsService metricsService;
    private List<SpinnerCheck> checks = new ArrayList<>();

    private boolean initialized = false;

    public final void init(TimeSource timeSource,
                           Spinner spinner,
                           Operation<?> operation,
                           ConcurrentCompletionTimeService completionTimeService,
                           ConcurrentErrorReporter errorReporter,
                           ConcurrentMetricsService metricsService) throws OperationException {
        if (initialized) {
            throw new OperationException(String.format("OperationHandler can not be initialized twice\n%s", toString()));
        }
        this.TIME_SOURCE = timeSource;
        this.spinner = spinner;
        this.operation = (OPERATION_TYPE) operation;
        this.completionTimeService = completionTimeService;
        this.errorReporter = errorReporter;
        this.metricsService = metricsService;

        this.initialized = true;
    }

    public final OPERATION_TYPE operation() {
        return operation;
    }

    public final ConcurrentCompletionTimeService completionTimeService() {
        return completionTimeService;
    }

    public final void setDbConnectionState(DbConnectionState dbConnectionState) {
        this.dbConnectionState = dbConnectionState;
    }

    public final DbConnectionState dbConnectionState() {
        return dbConnectionState;
    }

    public final void addCheck(SpinnerCheck check) {
        checks.add(check);
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
    public OperationResultReport call() {
        if (false == initialized) {
            errorReporter.reportError(this, "Handler was executed before being initialized");
            return null;
        }
        try {
            if (false == spinner.waitForScheduledStartTime(operation, new MultiCheck(checks))) {
                // TODO something more elaborate here? see comments in Spinner
                // Spinner result indicates operation should not be processed
                return null;
            }
            long startTimeAsMilli = TIME_SOURCE.nowAsMilli();
            OperationResultReport operationResultReport = executeOperation(operation);
            long finishTimeAsMilli = TIME_SOURCE.nowAsMilli();
            if (null == operationResultReport)
                throw new DbException(String.format("Handler returned null result:\n %s", toString()));
            operationResultReport.setRunDuration(Duration.fromMilli(finishTimeAsMilli - startTimeAsMilli));
            operationResultReport.setActualStartTime(Time.fromMilli(startTimeAsMilli));
            operationResultReport.setOperationType(operation.type());
            operationResultReport.setScheduledStartTime(operation.scheduledStartTime());
            completionTimeService.submitCompletedTime(operation.scheduledStartTime());
            metricsService.submitOperationResult(operationResultReport);
            return operationResultReport;
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

        return null;
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
}
