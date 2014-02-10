package com.ldbc.driver.runner;

import com.ldbc.driver.OperationResult;
import com.ldbc.driver.metrics.MetricException;
import com.ldbc.driver.metrics.WorkloadMetricsManager;

import java.util.concurrent.atomic.AtomicBoolean;

class MetricsLoggingThread extends Thread {
    private final WorkloadMetricsManager metricsManager;

    private final OperationHandlerExecutor operationHandlerExecutor;
    private AtomicBoolean isMoreResultsComing = new AtomicBoolean(true);
    private final ConcurrentErrorReporter concurrentErrorReporter;

    MetricsLoggingThread(OperationHandlerExecutor operationHandlerExecutor, WorkloadMetricsManager metricsManager, ConcurrentErrorReporter concurrentErrorReporter) {
        this.operationHandlerExecutor = operationHandlerExecutor;
        this.metricsManager = metricsManager;
        this.concurrentErrorReporter = concurrentErrorReporter;
    }

    final void finishLoggingRemainingResults() {
        isMoreResultsComing.set(false);
    }

    @Override
    public void run() {
        try {
            // Log results
            while (isMoreResultsComing.get()) {
                OperationResult operationResult = operationHandlerExecutor.nextOperationResultNonBlocking();
                if (null == operationResult) continue;
                log(operationResult);
            }
            // Log remaining results
            while (true) {
                OperationResult operationResult = operationHandlerExecutor.nextOperationResultBlocking();
                if (null == operationResult) break;
                log(operationResult);
            }
        } catch (MetricException e) {
            String errMsg = "Error encountered while logging metrics - logging thread exiting";
            concurrentErrorReporter.reportError(this, errMsg);
        } catch (OperationHandlerExecutorException e) {
            String errMsg = String.format("Error encountered while retrieving completed operation handler from executor - logging thread exiting\n%s",
                    ConcurrentErrorReporter.stackTraceToString(e));
            concurrentErrorReporter.reportError(this, errMsg);
        }
    }

    private void log(OperationResult operationResult) throws MetricException {
        try {
            metricsManager.measure(operationResult);
        } catch (Exception e) {
            String errMsg = String.format("Error encountered while logging result:\n\t%s", operationResult);
            throw new MetricException(errMsg, e.getCause());
        }
    }
}
