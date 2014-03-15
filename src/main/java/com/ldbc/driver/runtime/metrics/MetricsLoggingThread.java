//package com.ldbc.driver.runtime.metrics;
//
//import com.ldbc.driver.OperationResult;
//import com.ldbc.driver.runtime.error.ConcurrentErrorReporter;
//import com.ldbc.driver.runtime.executor.OperationHandlerExecutor;
//import com.ldbc.driver.runtime.executor.OperationHandlerExecutorException;
//import com.ldbc.driver.runtime.metrics_NEW.MetricsCollectionException;
//import com.ldbc.driver.runtime.metrics_NEW.WorkloadMetricsManager;
//
//import java.util.concurrent.atomic.AtomicBoolean;
//
//public class MetricsLoggingThread extends Thread {
//    private final WorkloadMetricsManager metricsManager;
//
//    private final OperationHandlerExecutor operationHandlerExecutor;
//    private AtomicBoolean isMoreResultsComing = new AtomicBoolean(true);
//    private final ConcurrentErrorReporter concurrentErrorReporter;
//
//    public MetricsLoggingThread(OperationHandlerExecutor operationHandlerExecutor,
//                                WorkloadMetricsManager metricsManager,
//                                ConcurrentErrorReporter concurrentErrorReporter) {
//        this.operationHandlerExecutor = operationHandlerExecutor;
//        this.metricsManager = metricsManager;
//        this.concurrentErrorReporter = concurrentErrorReporter;
//    }
//
//    public final void finishLoggingRemainingResults() {
//        isMoreResultsComing.set(false);
//    }
//
//    @Override
//    public void run() {
//        try {
//            // Log results
//            while (isMoreResultsComing.get()) {
//                OperationResult operationResult = operationHandlerExecutor.poll();
//                if (null == operationResult) continue;
//                log(operationResult);
//            }
//            // Log remaining results
//            while (true) {
//                OperationResult operationResult = operationHandlerExecutor.take();
//                if (null == operationResult) break;
//                log(operationResult);
//            }
//        } catch (MetricsCollectionException e) {
//            String errMsg = "Error encountered while logging metrics - logging thread exiting";
//            concurrentErrorReporter.reportError(this, errMsg);
//        } catch (OperationHandlerExecutorException e) {
//            String errMsg = String.format("Error encountered while retrieving completed operation handler from executor - logging thread exiting\n%s",
//                    ConcurrentErrorReporter.stackTraceToString(e));
//            concurrentErrorReporter.reportError(this, errMsg);
//        }
//    }
//
//    private void log(OperationResult operationResult) throws MetricsCollectionException {
//        try {
//            metricsManager.measure(operationResult);
//        } catch (Exception e) {
//            String errMsg = String.format("Error encountered while logging result:\n\t%s", operationResult);
//            throw new MetricsCollectionException(errMsg, e.getCause());
//        }
//    }
//}
