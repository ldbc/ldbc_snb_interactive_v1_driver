package org.ldbcouncil.snb.driver.validation;
/**
 * ResultsLogValidator.java
 * This class computes and validates the results of the benchmark. Compute reads the result file
 * and records any delayed operation. Validate checks from the computed summary if it exceeds the
 * threshold.
 */

import org.ldbcouncil.snb.driver.csv.simple.SimpleCsvFileReader;
import org.ldbcouncil.snb.driver.runtime.metrics.OperationMetricsSnapshot;
import org.ldbcouncil.snb.driver.runtime.metrics.WorkloadResultsSnapshot;
import org.ldbcouncil.snb.driver.temporal.TemporalUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import static org.ldbcouncil.snb.driver.validation.ResultsLogValidationResult.ValidationErrorType;
import static java.lang.String.format;

public class ResultsLogValidator
{
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();

    /**
     * Validates the result from the benchmark. Checks the amount of 
     * @param summary Summary of delayed operations previously computed @see {@link org.ldbcouncil.snb.driver.validation.ResultsLogValidator#compute(File, long)}
     * @param tolerances The resultsLogValidationTolerances object including the tolerated
     * delay count. @see {@link org.ldbcouncil.snb.driver.Workload#ResultsLogValidationTolerances(org.ldbcouncil.snb.driver.control.DriverConfiguration, boolean)}
     * @param recordDelayedOperations Record the late operations even when the total late operations are under the threshold
     * @return ResultsLogValidationResult containing the list of delayed operations
     */
    public ResultsLogValidationResult validate(
            ResultsLogValidationSummary summary,
            ResultsLogValidationTolerances tolerances,
            boolean recordDelayedOperations,
            WorkloadResultsSnapshot workloadResults
    )
    {
        ResultsLogValidationResult result = new ResultsLogValidationResult();

        Map<String, Long> operationCountPerTypeMap = new HashMap<>();
        for (OperationMetricsSnapshot metric : workloadResults.allMetrics())
        {
            operationCountPerTypeMap.put(metric.name(), metric.count());
        }

        for ( String operationType : summary.excessiveDelayCountPerType().keySet() )
        {
            long allowedLateOperations = Math.round(operationCountPerTypeMap.get(operationType) * tolerances.toleratedExcessiveDelayCountPercentage());
            if (recordDelayedOperations && summary.excessiveDelayCountPerType().get( operationType ) > allowedLateOperations )
            {
                result.aboveThreshold();
                result.addError(
                    ValidationErrorType.TOO_MANY_LATE_OPERATIONS,
                    format( "Late Count for %s (%s) > (%s) Tolerated Late Count",
                            operationType,
                            summary.excessiveDelayCountPerType().get( operationType ),
                            allowedLateOperations
                    )
                );
            }
        }
        return result;
    }

    /***
     * Loads the benchmark result file and uses the ResultsLogValidationSummaryCalculator to record delayed
     * operations.
     * @param resultsLog The File object to the operation result log CSV-file.
     * @param excessiveDelayThresholdAsMilli The delay threshold when an operation is considered delayed.
     * @return Summary of the delayed operations in a ResultsLogValidationSummary object
     * @throws ValidationException When the result CSV file could not be opened or invalid delay is computed.
     */
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
        // Create summary
        return calculator.snapshot();
    }

    /**
     * Calculates the maximum delay in the results used to place results in the Histogram object.
     * @param resultsLog The File object to the operation result log CSV-file.
     * @return maximum delay found in the result file.
     * @throws ValidationException When the delay is invalid (negative)
     */
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
