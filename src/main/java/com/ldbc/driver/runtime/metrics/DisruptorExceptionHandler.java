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
                String.format("%s encountered error on event\nl = %s\no = %s\n%s",
                        DisruptorJavolutionMetricsService.class.getSimpleName(),
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
                        DisruptorJavolutionMetricsService.class.getSimpleName(),
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
                        DisruptorJavolutionMetricsService.class.getSimpleName(),
                        ConcurrentErrorReporter.stackTraceToString(throwable)
                )
        );
        // TODO remove
        throwable.printStackTrace();
    }
}
