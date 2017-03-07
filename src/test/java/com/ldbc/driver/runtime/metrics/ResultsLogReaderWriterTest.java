package com.ldbc.driver.runtime.metrics;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ResultsLogReaderWriterTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void shouldWriteAndReadSameValues() throws Exception
    {
        File resultsLog = temporaryFolder.newFile();
        TimeUnit unit = NANOSECONDS;

        try ( ResultsLogWriter writer = new SimpleResultsLogWriter( resultsLog, unit ) )
        {
            writer.write(
                    "a",
                    Long.MAX_VALUE,
                    Long.MAX_VALUE,
                    Long.MAX_VALUE,
                    Integer.MAX_VALUE,
                    Long.MAX_VALUE );
            writer.write(
                    "b",
                    0,
                    0,
                    0,
                    Integer.MIN_VALUE,
                    0 );
        }

        try ( ResultsLogReader reader = new SimpleResultsLogReader( resultsLog ) )
        {
            assertTrue( reader.next() );
            assertThat( reader.getOperationName(), equalTo( "a" ) );
            assertThat( reader.getScheduledStartTimeAsMilli(), equalTo( Long.MAX_VALUE ) );
            assertThat( reader.getActualStartTimeAsMilli(), equalTo( Long.MAX_VALUE ) );
            assertThat( reader.getRunDurationAsNano(), equalTo( Long.MAX_VALUE ) );
            assertThat( reader.getResultCode(), equalTo( Integer.MAX_VALUE ) );
            assertThat( reader.getOriginalStartTime(), equalTo( Long.MAX_VALUE ) );

            assertTrue( reader.next() );
            assertThat( reader.getOperationName(), equalTo( "b" ) );
            assertThat( reader.getScheduledStartTimeAsMilli(), equalTo( 0L ) );
            assertThat( reader.getActualStartTimeAsMilli(), equalTo( 0L ) );
            assertThat( reader.getRunDurationAsNano(), equalTo( 0L ) );
            assertThat( reader.getResultCode(), equalTo( Integer.MIN_VALUE ) );
            assertThat( reader.getOriginalStartTime(), equalTo( 0L ) );

            assertFalse( reader.next() );
        }
    }

    @Test
    public void shouldConvertUnitsCorrectly() throws Exception
    {
        File resultsLog = temporaryFolder.newFile();
        TimeUnit unit = MILLISECONDS;

        try ( ResultsLogWriter writer = new SimpleResultsLogWriter( resultsLog, unit ) )
        {
            writer.write(
                    "a",
                    Long.MAX_VALUE,
                    Long.MAX_VALUE,
                    Long.MAX_VALUE,
                    Integer.MAX_VALUE,
                    Long.MAX_VALUE );
            writer.write(
                    "b",
                    0,
                    0,
                    NANOSECONDS.convert( 0, unit ),
                    Integer.MIN_VALUE,
                    0 );
        }

        try ( ResultsLogReader reader = new SimpleResultsLogReader( resultsLog ) )
        {
            assertTrue( reader.next() );
            assertThat( reader.getOperationName(), equalTo( "a" ) );
            assertThat( reader.getScheduledStartTimeAsMilli(), equalTo( Long.MAX_VALUE ) );
            assertThat( reader.getActualStartTimeAsMilli(), equalTo( Long.MAX_VALUE ) );
            assertThat( reader.getRunDurationAsNano(),
                    equalTo( NANOSECONDS.convert( unit.convert( Long.MAX_VALUE, NANOSECONDS ), unit ) ) );
            assertThat( reader.getResultCode(), equalTo( Integer.MAX_VALUE ) );
            assertThat( reader.getOriginalStartTime(), equalTo( Long.MAX_VALUE ) );

            assertTrue( reader.next() );
            assertThat( reader.getOperationName(), equalTo( "b" ) );
            assertThat( reader.getScheduledStartTimeAsMilli(), equalTo( 0L ) );
            assertThat( reader.getActualStartTimeAsMilli(), equalTo( 0L ) );
            assertThat( reader.getRunDurationAsNano(),
                    equalTo( NANOSECONDS.convert( unit.convert( 0, NANOSECONDS ), unit ) ) );
            assertThat( reader.getResultCode(), equalTo( Integer.MIN_VALUE ) );
            assertThat( reader.getOriginalStartTime(), equalTo( 0L ) );

            assertFalse( reader.next() );
        }
    }
}
