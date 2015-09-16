package com.ldbc.driver.validation;

import com.google.common.collect.Lists;
import com.ldbc.driver.csv.simple.SimpleCsvFileWriter;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple2;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ResultsLogValidationTest
{
    private static final List<Tuple2<String,Long>> DELAYS = Lists.newArrayList(
            Tuple.tuple2( "A", 1l ),
            Tuple.tuple2( "B", 1l ),
            Tuple.tuple2( "B", 1l ),
            Tuple.tuple2( "C", 4l ),
            Tuple.tuple2( "B", 5l ),
            Tuple.tuple2( "C", 6l ),
            Tuple.tuple2( "C", 10l ),
            Tuple.tuple2( "B", 11l ),
            Tuple.tuple2( "D", 1000l ),
            Tuple.tuple2( "E", 10000l )
    );
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void shouldPassValidationWhenResultsAreGood()
    {
        long excessiveDelayThresholdAsMilli = 10;
        long excessiveDelayCount = 10;
        Map<String,Long> excessiveDelayCountPerType = new HashMap<>();
        excessiveDelayCountPerType.put( "A", 1l );
        excessiveDelayCountPerType.put( "B", 2l );
        excessiveDelayCountPerType.put( "C", 3l );
        long minDelayAsMilli = 0;
        long maxDelayAsMilli = 0;
        long meanDelayAsMilli = 0;
        Map<String,Long> minDelayAsMilliPerType = new HashMap<>();
        Map<String,Long> maxDelayAsMilliPerType = new HashMap<>();
        Map<String,Long> meanDelayAsMilliPerType = new HashMap<>();
        ResultsLogValidationSummary summary = new ResultsLogValidationSummary(
                excessiveDelayThresholdAsMilli,
                excessiveDelayCount,
                excessiveDelayCountPerType,
                minDelayAsMilli,
                maxDelayAsMilli,
                meanDelayAsMilli,
                minDelayAsMilliPerType,
                maxDelayAsMilliPerType,
                meanDelayAsMilliPerType
        );

        long toleratedExcessiveDelayCount = excessiveDelayCount;
        Map<String,Long> toleratedExcessiveDelayCountPerType = new HashMap<>();
        toleratedExcessiveDelayCountPerType.put( "A", excessiveDelayCountPerType.get( "A" ) );
        toleratedExcessiveDelayCountPerType.put( "B", excessiveDelayCountPerType.get( "B" ) );
        toleratedExcessiveDelayCountPerType.put( "C", excessiveDelayCountPerType.get( "C" ) );
        ResultsLogValidator validator = new ResultsLogValidator();
        ResultsLogValidationTolerances tolerances = new ResultsLogValidationTolerances(
                excessiveDelayThresholdAsMilli,
                toleratedExcessiveDelayCount,
                toleratedExcessiveDelayCountPerType
        );
        ResultsLogValidationResult result = validator.validate(
                summary,
                tolerances
        );
        assertTrue( result.toString(), result.isSuccessful() );
    }

    @Test
    public void shouldFailValidationWhenExcessiveDelayCountIsExceeded()
    {
        long excessiveDelayThresholdAsMilli = 10;
        long excessiveDelayCount = 10;
        Map<String,Long> excessiveDelayCountPerType = new HashMap<>();
        excessiveDelayCountPerType.put( "A", 1l );
        excessiveDelayCountPerType.put( "B", 2l );
        excessiveDelayCountPerType.put( "C", 3l );
        long minDelayAsMilli = 0;
        long maxDelayAsMilli = 0;
        long meanDelayAsMilli = 0;
        Map<String,Long> minDelayAsMilliPerType = new HashMap<>();
        Map<String,Long> maxDelayAsMilliPerType = new HashMap<>();
        Map<String,Long> meanDelayAsMilliPerType = new HashMap<>();
        ResultsLogValidationSummary summary = new ResultsLogValidationSummary(
                excessiveDelayThresholdAsMilli,
                excessiveDelayCount,
                excessiveDelayCountPerType,
                minDelayAsMilli,
                maxDelayAsMilli,
                meanDelayAsMilli,
                minDelayAsMilliPerType,
                maxDelayAsMilliPerType,
                meanDelayAsMilliPerType
        );

        long toleratedExcessiveDelayCount = excessiveDelayCount - 1;
        Map<String,Long> toleratedExcessiveDelayCountPerType = new HashMap<>();
        toleratedExcessiveDelayCountPerType.put( "A", excessiveDelayCountPerType.get( "A" ) );
        toleratedExcessiveDelayCountPerType.put( "B", excessiveDelayCountPerType.get( "B" ) );
        toleratedExcessiveDelayCountPerType.put( "C", excessiveDelayCountPerType.get( "C" ) );
        ResultsLogValidator validator = new ResultsLogValidator();
        ResultsLogValidationTolerances tolerances = new ResultsLogValidationTolerances(
                excessiveDelayThresholdAsMilli,
                toleratedExcessiveDelayCount,
                toleratedExcessiveDelayCountPerType
        );
        ResultsLogValidationResult result = validator.validate(
                summary,
                tolerances
        );
        assertFalse( result.toString(), result.isSuccessful() );
        Assert.assertThat( result.toString(), result.errors().size(), equalTo( 1 ) );
        Assert.assertThat(
                result.toString(),
                result.errors().get( 0 ).errorType(),
                equalTo( ResultsLogValidationResult.ValidationErrorType.TOO_MANY_LATE_OPERATIONS )
        );
    }

    @Test
    public void shouldFailValidationWhenExcessiveDelayCountPerTypeIsExceeded()
    {
        long excessiveDelayThresholdAsMilli = 10;
        long excessiveDelayCount = 10;
        Map<String,Long> excessiveDelayCountPerType = new HashMap<>();
        excessiveDelayCountPerType.put( "A", 1l );
        excessiveDelayCountPerType.put( "B", 2l );
        excessiveDelayCountPerType.put( "C", 3l );
        long minDelayAsMilli = 0;
        long maxDelayAsMilli = 0;
        long meanDelayAsMilli = 0;
        Map<String,Long> minDelayAsMilliPerType = new HashMap<>();
        Map<String,Long> maxDelayAsMilliPerType = new HashMap<>();
        Map<String,Long> meanDelayAsMilliPerType = new HashMap<>();
        ResultsLogValidationSummary summary = new ResultsLogValidationSummary(
                excessiveDelayThresholdAsMilli,
                excessiveDelayCount,
                excessiveDelayCountPerType,
                minDelayAsMilli,
                maxDelayAsMilli,
                meanDelayAsMilli,
                minDelayAsMilliPerType,
                maxDelayAsMilliPerType,
                meanDelayAsMilliPerType
        );

        long toleratedExcessiveDelayCount = excessiveDelayCount;
        Map<String,Long> toleratedExcessiveDelayCountPerType = new HashMap<>();
        toleratedExcessiveDelayCountPerType.put( "A", excessiveDelayCountPerType.get( "A" ) );
        toleratedExcessiveDelayCountPerType.put( "B", excessiveDelayCountPerType.get( "B" ) - 1 );
        toleratedExcessiveDelayCountPerType.put( "C", excessiveDelayCountPerType.get( "C" ) );
        ResultsLogValidator validator = new ResultsLogValidator();
        ResultsLogValidationTolerances tolerances = new ResultsLogValidationTolerances(
                excessiveDelayThresholdAsMilli,
                toleratedExcessiveDelayCount,
                toleratedExcessiveDelayCountPerType
        );
        ResultsLogValidationResult result = validator.validate(
                summary,
                tolerances
        );
        assertFalse( result.toString(), result.isSuccessful() );
        Assert.assertThat( result.toString(), result.errors().size(), equalTo( 1 ) );
        Assert.assertThat(
                result.toString(),
                result.errors().get( 0 ).errorType(),
                equalTo( ResultsLogValidationResult.ValidationErrorType.TOO_MANY_LATE_OPERATIONS_FOR_TYPE )
        );
    }

    @Test
    public void shouldReturnExpectedSummaryWhenComputedThenSerializedAndMarshaled() throws IOException
    {
        // Given
        long excessiveDelayThreshold = 5;
        ResultsLogValidationSummaryCalculator calculator = new ResultsLogValidationSummaryCalculator(
                10000,
                excessiveDelayThreshold
        );

        // When
        for ( Tuple2<String,Long> delay : DELAYS )
        {
            calculator.recordDelay( delay._1(), delay._2() );
        }

        ResultsLogValidationSummary summary = calculator.snapshot();
        String serializedSummary = summary.toJson();
        System.out.println( serializedSummary );
        ResultsLogValidationSummary summaryAfterMarshal = ResultsLogValidationSummary.fromJson(
                serializedSummary
        );

        // Then
        doSummaryAsserts( summary );
        doSummaryAsserts( summaryAfterMarshal );
    }

    @Test
    public void shouldReturnExpectedSummaryWhenValidatedFromFile() throws IOException, ValidationException
    {
        // Given
        long excessiveDelayThreshold = 5;
        File file = temporaryFolder.newFile();
        try ( SimpleCsvFileWriter writer =
                      new SimpleCsvFileWriter( file, SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR ) )
        {
            writer.writeRow(
                    "operation_type",
                    "scheduled_start_time",
                    "actual_start_time",
                    "duration",
                    "result_code"
            );
            for ( Tuple2<String,Long> delay : DELAYS )
            {
                writer.writeRow(
                        // operation type
                        delay._1(),
                        // scheduled start time
                        Long.toString( 0 ),
                        // actual start time
                        Long.toString( delay._2() ),
                        // duration
                        Long.toString( 0 ),
                        // result code
                        Long.toString( 0 )
                );
            }
        }

        // When
        ResultsLogValidator validator = new ResultsLogValidator();
        ResultsLogValidationSummary summary = validator.compute( file, excessiveDelayThreshold );
        String serializedSummary = summary.toJson();
        System.out.println( serializedSummary );
        ResultsLogValidationSummary summaryAfterMarshal = ResultsLogValidationSummary.fromJson(
                serializedSummary
        );

        // Then
        doSummaryAsserts( summary );
        doSummaryAsserts( summaryAfterMarshal );
    }

    private void doSummaryAsserts( ResultsLogValidationSummary summary )
    {
        assertThat( summary.excessiveDelayThresholdAsMilli(), equalTo( 5l ) );
        assertThat( summary.excessiveDelayCount(), equalTo( 5l ) );
        assertThat(
                format( "Found: %s", summary.excessiveDelayCountPerType().keySet().toString() ),
                summary.excessiveDelayCountPerType().size(), equalTo( 5 )
        );
        assertThat( summary.excessiveDelayCountPerType().get( "A" ), equalTo( 0l ) );
        assertThat( summary.excessiveDelayCountPerType().get( "B" ), equalTo( 1l ) );
        assertThat( summary.excessiveDelayCountPerType().get( "C" ), equalTo( 2l ) );
        assertThat( summary.excessiveDelayCountPerType().get( "D" ), equalTo( 1l ) );
        assertThat( summary.excessiveDelayCountPerType().get( "E" ), equalTo( 1l ) );
        assertThat( summary.minDelayAsMilli(), equalTo( 1l ) );
        assertThat( summary.maxDelayAsMilli(), equalTo( 10000l ) );
        assertThat( summary.meanDelayAsMilli(), equalTo( 1104l ) );
        assertThat( summary.minDelayAsMilliPerType().size(), equalTo( 5 ) );
        assertThat( summary.minDelayAsMilliPerType().get( "A" ), equalTo( 1l ) );
        assertThat( summary.minDelayAsMilliPerType().get( "B" ), equalTo( 1l ) );
        assertThat( summary.minDelayAsMilliPerType().get( "C" ), equalTo( 4l ) );
        assertThat( summary.minDelayAsMilliPerType().get( "D" ), equalTo( 1000l ) );
        assertThat( summary.minDelayAsMilliPerType().get( "E" ), equalTo( 10000l ) );
        assertThat( summary.maxDelayAsMilliPerType().size(), equalTo( 5 ) );
        assertThat( summary.maxDelayAsMilliPerType().get( "A" ), equalTo( 1l ) );
        assertThat( summary.maxDelayAsMilliPerType().get( "B" ), equalTo( 11l ) );
        assertThat( summary.maxDelayAsMilliPerType().get( "C" ), equalTo( 10l ) );
        assertThat( summary.maxDelayAsMilliPerType().get( "D" ), equalTo( 1000l ) );
        assertThat( summary.maxDelayAsMilliPerType().get( "E" ), equalTo( 10000l ) );
        assertThat( summary.meanDelayAsMilliPerType().size(), equalTo( 5 ) );
        assertThat( summary.meanDelayAsMilliPerType().get( "A" ), equalTo( 1l ) );
        assertThat( summary.meanDelayAsMilliPerType().get( "B" ), equalTo( 5l ) );
        assertThat( summary.meanDelayAsMilliPerType().get( "C" ), equalTo( 7l ) );
        assertThat( summary.meanDelayAsMilliPerType().get( "D" ), equalTo( 1000l ) );
        assertThat( summary.meanDelayAsMilliPerType().get( "E" ), equalTo( 10000l ) );
    }
}
