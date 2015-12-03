package com.ldbc.driver.validation;

import com.ldbc.driver.csv.simple.SimpleCsvFileReader;
import com.ldbc.driver.temporal.TemporalUtil;

import java.io.File;
import java.io.FileNotFoundException;

import static com.ldbc.driver.validation.ResultsLogValidationResult.ValidationErrorType;
import static java.lang.String.format;

public class ResultsLogValidator
{
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();

    public ResultsLogValidationResult validate(
            ResultsLogValidationSummary summary,
            ResultsLogValidationTolerances tolerances )
    {
        ResultsLogValidationResult result = new ResultsLogValidationResult();
        if ( summary.excessiveDelayCount() > tolerances.toleratedExcessiveDelayCount() )
        {
            result.addError(
                    ValidationErrorType.TOO_MANY_LATE_OPERATIONS,
                    format( "Late Count (%s) > (%s) Tolerated Late Count",
                            summary.excessiveDelayCount(),
                            tolerances.toleratedExcessiveDelayCount() )
            );
        }
        for ( String operationType : summary.excessiveDelayCountPerType().keySet() )
        {
            long excessiveDelayCountForOperationType = summary.excessiveDelayCountPerType().get( operationType );
            if ( tolerances.toleratedExcessiveDelayCountPerType().containsKey( operationType ) &&
                 tolerances.toleratedExcessiveDelayCountPerType().get( operationType ) <
                 excessiveDelayCountForOperationType )
            {
                result.addError(
                        ValidationErrorType.TOO_MANY_LATE_OPERATIONS_FOR_TYPE,
                        format( "Late Count for %s (%s) > (%s) Tolerated Late Count",
                                operationType,
                                summary.excessiveDelayCountPerType().get( operationType ),
                                tolerances.toleratedExcessiveDelayCountPerType().get( operationType ) )
                );
            }
        }
        return result;
    }

    public ResultsLogValidationSummary compute( File resultsLog, long excessiveDelayThresholdAsMilli )
            throws ValidationException
    {
        long maxDelayAsMilli = maxDelayAsMilli( resultsLog );
        ResultsLogValidationSummaryCalculator calculator = new ResultsLogValidationSummaryCalculator(
                maxDelayAsMilli,
                excessiveDelayThresholdAsMilli
        );

        try ( SimpleCsvFileReader reader = new SimpleCsvFileReader(
                resultsLog,
                SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING ) )
        {
            // skip headers
            reader.next();
            while ( reader.hasNext() )
            {
                String[] row = reader.next();
                String operationType = row[0];
                long scheduledStartTimeAsMilli = Long.parseLong( row[1] );
                long actualStartTimeAsMilli = Long.parseLong( row[2] );
                // duration
                // result code
                long delayAsMilli = actualStartTimeAsMilli - scheduledStartTimeAsMilli;
                calculator.recordDelay( operationType, delayAsMilli );
            }
        }
        catch ( FileNotFoundException e )
        {
            throw new ValidationException( format( "Error opening results log: %s", resultsLog.getAbsolutePath() ), e );
        }

        return calculator.snapshot();
    }

    private long maxDelayAsMilli( File resultsLog ) throws ValidationException
    {
        long maxDelayAsMilli = 0;
        try ( SimpleCsvFileReader reader = new SimpleCsvFileReader(
                resultsLog,
                SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING ) )
        {
            // skip headers
            reader.next();
            while ( reader.hasNext() )
            {
                String[] row = reader.next();
                // operation type
                long scheduledStartTimeAsMilli = Long.parseLong( row[1] );
                long actualStartTimeAsMilli = Long.parseLong( row[2] );
                // duration
                // result code
                long delayAsMilli = actualStartTimeAsMilli - scheduledStartTimeAsMilli;
                if ( delayAsMilli < 0 )
                {
                    throw new ValidationException(
                            format( "Delay can not be negative\n" +
                                    "Delay: %s (ms) / %s\n" +
                                    "Scheduled Start Time: %s (ms) / %s\n" +
                                    "Actual Start Time: %s (ms) / %s",
                                    delayAsMilli,
                                    TEMPORAL_UTIL.milliDurationToString( delayAsMilli ),
                                    scheduledStartTimeAsMilli,
                                    TEMPORAL_UTIL.milliTimeToTimeString( scheduledStartTimeAsMilli ),
                                    actualStartTimeAsMilli,
                                    TEMPORAL_UTIL.milliTimeToTimeString( actualStartTimeAsMilli )
                            )
                    );
                }
                if ( delayAsMilli > maxDelayAsMilli )
                {
                    maxDelayAsMilli = delayAsMilli;
                }
            }
        }
        catch ( FileNotFoundException e )
        {
            throw new ValidationException( format( "Error opening results log: %s", resultsLog.getAbsolutePath() ), e );
        }
        return maxDelayAsMilli;
    }
}
