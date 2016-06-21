package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.lmax.disruptor.ExceptionHandler;

import static java.lang.String.format;

public class DisruptorExceptionHandler implements ExceptionHandler
{
    private final ConcurrentErrorReporter errorReporter;

    public DisruptorExceptionHandler( ConcurrentErrorReporter errorReporter )
    {
        this.errorReporter = errorReporter;
    }

    @Override
    public void handleEventException( Throwable throwable, long l, Object o )
    {
        errorReporter.reportError(
                this,
                format( "Disruptor encountered error on event\nl = %s\no = %s\n%s",
                        l,
                        o.toString(),
                        ConcurrentErrorReporter.stackTraceToString( throwable )
                )
        );
    }

    @Override
    public void handleOnStartException( Throwable throwable )
    {
        errorReporter.reportError(
                this,
                format( "Disruptor encountered error on start\n%s",
                        ConcurrentErrorReporter.stackTraceToString( throwable )
                )
        );
    }

    @Override
    public void handleOnShutdownException( Throwable throwable )
    {
        errorReporter.reportError(
                this,
                format( "%s encountered error on shutdown\n%s",
                        DisruptorSbeMetricsService.class.getSimpleName(),
                        ConcurrentErrorReporter.stackTraceToString( throwable )
                )
        );
    }
}
