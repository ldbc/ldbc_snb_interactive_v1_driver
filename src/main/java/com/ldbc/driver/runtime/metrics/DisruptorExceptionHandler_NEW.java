package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.lmax.disruptor.ExceptionHandler;

public class DisruptorExceptionHandler_NEW implements ExceptionHandler {
    private final ConcurrentErrorReporter errorReporter;

    public DisruptorExceptionHandler_NEW(ConcurrentErrorReporter errorReporter) {
        this.errorReporter = errorReporter;
    }

    @Override
    public void handleEventException(Throwable throwable, long l, Object o) {
        errorReporter.reportError(
                this,
                String.format("%s encountered error on event\nl = %s\no = %s\n%s",
                        DisruptorConcurrentMetricsService.class.getSimpleName(),
                        l,
                        o.toString(),
                        ConcurrentErrorReporter.stackTraceToString(throwable)
                )
        );
        // TODO remove
        throwable.printStackTrace();
    }

    @Override
    public void handleOnStartException(Throwable throwable) {
        errorReporter.reportError(
                this,
                String.format("%s encountered error on start\n%s",
                        DisruptorConcurrentMetricsService.class.getSimpleName(),
                        ConcurrentErrorReporter.stackTraceToString(throwable)
                )
        );
        // TODO remove
        throwable.printStackTrace();
    }

    @Override
    public void handleOnShutdownException(Throwable throwable) {
        errorReporter.reportError(
                this,
                String.format("%s encountered error on shutdown\n%s",
                        DisruptorConcurrentMetricsService.class.getSimpleName(),
                        ConcurrentErrorReporter.stackTraceToString(throwable)
                )
        );
        // TODO remove
        throwable.printStackTrace();
    }
}
