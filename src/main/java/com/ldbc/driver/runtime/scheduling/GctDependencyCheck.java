package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;
import com.ldbc.driver.temporal.TemporalUtil;

import static java.lang.String.format;

public class GctDependencyCheck implements SpinnerCheck
{
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
    private final GlobalCompletionTimeReader globalCompletionTimeReader;
    private final ConcurrentErrorReporter errorReporter;

    public GctDependencyCheck(
            GlobalCompletionTimeReader globalCompletionTimeReader,
            ConcurrentErrorReporter errorReporter )
    {
        this.globalCompletionTimeReader = globalCompletionTimeReader;
        this.errorReporter = errorReporter;
    }

    @Override
    public SpinnerCheckResult doCheck( Operation operation )
    {
        try
        {
            return (globalCompletionTimeReader.globalCompletionTimeAsMilli() >= operation.dependencyTimeStamp())
                   ? SpinnerCheckResult.PASSED : SpinnerCheckResult.STILL_CHECKING;
        }
        catch ( CompletionTimeException e )
        {
            errorReporter.reportError( this,
                    format(
                            "Error encountered while reading GCT for query %s\n%s",
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
            // Note, GCT printed here may be a little later than GCT that was measured during check
            errorReporter.reportError( this,
                    format( "GCT(%s) has not advanced sufficiently to execute operation\n"
                            + "Operation: %s\n"
                            + "Time Stamp: %s\n"
                            + "Dependency Time Stamp: %s",
                            TEMPORAL_UTIL.milliTimeToDateTimeString(
                                    globalCompletionTimeReader.globalCompletionTimeAsMilli()
                            ),
                            operation,
                            operation.timeStamp(),
                            operation.dependencyTimeStamp() ) );
            return false;
        }
        catch ( CompletionTimeException e )
        {
            errorReporter.reportError( this,
                    format(
                            "Error encountered in handleFailedCheck while reading GCT for query %s\n%s",
                            operation.getClass().getSimpleName(),
                            ConcurrentErrorReporter.stackTraceToString( e ) ) );
            return false;
        }
    }
}
