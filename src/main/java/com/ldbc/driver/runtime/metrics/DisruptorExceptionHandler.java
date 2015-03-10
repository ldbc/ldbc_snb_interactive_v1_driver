package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.lmax.disruptor.ExceptionHandler;

public class DisruptorExceptionHandler implements ExceptionHandler {
    private final ConcurrentErrorReporter errorReporter;

    public DisruptorExceptionHandler(ConcurrentErrorReporter errorReporter) {
        this.errorReporter = errorReporter;
    }

    @Override
    public void handleEventException(Throwable throwable, long l, Object o) {
        errorReporter.reportError(
                this,
                String.format("Disruptor encountered error on event\nl = %s\no = %s\n%s",
                        l,
                        o.toString(),
                        ConcurrentErrorReporter.stackTraceToString(throwable)
                )
        );
    }

    @Override
    public void handleOnStartException(Throwable throwable) {
        errorReporter.reportError(
                this,
                String.format("Disruptor encountered error on start\n%s",
                        ConcurrentErrorReporter.stackTraceToString(throwable)
                )
        );
    }

    @Override
    public void handleOnShutdownException(Throwable throwable) {
        errorReporter.reportError(
                this,
                String.format("%s encountered error on shutdown\n%s",
                        DisruptorJavolutionMetricsService.class.getSimpleName(),
                        ConcurrentErrorReporter.stackTraceToString(throwable)
                )
        );
    }
}
