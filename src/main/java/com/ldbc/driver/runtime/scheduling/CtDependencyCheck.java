package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.CompletionTimeReader;
import com.ldbc.driver.temporal.TemporalUtil;

import static java.lang.String.format;

public class CtDependencyCheck implements SpinnerCheck
{
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
    private final CompletionTimeReader completionTimeReader;
    private final ConcurrentErrorReporter errorReporter;

    public CtDependencyCheck( CompletionTimeReader completionTimeReader, ConcurrentErrorReporter errorReporter )
    {
        this.completionTimeReader = completionTimeReader;
        this.errorReporter = errorReporter;
    }

    @Override
    public SpinnerCheckResult doCheck( Operation operation )
    {
        try
        {
            return (completionTimeReader.completionTimeAsMilli() >= operation.dependencyTimeStamp())
                   ? SpinnerCheckResult.PASSED : SpinnerCheckResult.STILL_CHECKING;
        }
        catch ( CompletionTimeException e )
        {
            errorReporter.reportError( this,
                    format(
                            "Error encountered while reading CT for query %s\n%s",
                            operation.getClass().getSimpleName(),
                            ConcurrentErrorReporter.stackTraceToString( e ) ) );
            return SpinnerCheckResult.FAILED;
        }
    }

    @Override
    public boolean handleFailedCheck( Operation operation )
    {
        try
        {
            // Note, CT printed here may be a little later than CT that was measured during check
            errorReporter.reportError( this,
                    format( "CT(%s) has not advanced sufficiently to execute operation\n"
                            + "Operation: %s\n"
                            + "Time Stamp: %s\n"
                            + "Dependency Time Stamp: %s",
                            TEMPORAL_UTIL.milliTimeToDateTimeString( completionTimeReader.completionTimeAsMilli() ),
                            operation,
                            operation.timeStamp(),
                            operation.dependencyTimeStamp() ) );
            return false;
        }
        catch ( CompletionTimeException e )
        {
            errorReporter.reportError( this,
                    format(
                            "Error encountered in handleFailedCheck while reading CT for query %s\n%s",
                            operation.getClass().getSimpleName(),
                            ConcurrentErrorReporter.stackTraceToString( e ) ) );
            return false;
        }
    }
}
