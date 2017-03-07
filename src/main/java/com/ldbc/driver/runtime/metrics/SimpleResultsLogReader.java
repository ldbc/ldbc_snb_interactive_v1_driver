package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.csv.simple.SimpleCsvFileReader;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.ldbc.driver.runtime.metrics.ResultsLogWriter.HEADER_EXECUTION_DURATION_PREFIX;
import static com.ldbc.driver.runtime.metrics.ResultsLogWriter.INDEX_ACTUAL_START_TIME;
import static com.ldbc.driver.runtime.metrics.ResultsLogWriter.INDEX_EXECUTION_DURATION;
import static com.ldbc.driver.runtime.metrics.ResultsLogWriter.INDEX_OPERATION_TYPE;
import static com.ldbc.driver.runtime.metrics.ResultsLogWriter.INDEX_ORIGINAL_START_TIME;
import static com.ldbc.driver.runtime.metrics.ResultsLogWriter.INDEX_RESULT_CODE;
import static com.ldbc.driver.runtime.metrics.ResultsLogWriter.INDEX_SCHEDULED_START_TIME;
import static java.util.concurrent.TimeUnit.valueOf;

public class SimpleResultsLogReader implements ResultsLogReader
{
    private final SimpleCsvFileReader reader;
    private final TimeUnit unit;
    private String[] row = null;

    public SimpleResultsLogReader( File resultsLog ) throws IOException
    {
        this.reader = new SimpleCsvFileReader( resultsLog, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING );
        this.unit = processHeaders();
    }

    private TimeUnit processHeaders()
    {
        if ( reader.hasNext() )
        {
            String[] headers = reader.next();
            if ( !headers[INDEX_SCHEDULED_START_TIME].equals( ResultsLogWriter.HEADER_SCHEDULED_START_TIME ) )
            {
                throw new RuntimeException( "Expected first row to be headers but was: " + Arrays.toString( headers ) );
            }
            return valueOf( headers[INDEX_EXECUTION_DURATION].replace( HEADER_EXECUTION_DURATION_PREFIX, "" ) );
        }
        else
        {
            throw new RuntimeException( "Expected first row to be headers but file was empty" );
        }
    }

    @Override
    public boolean next()
    {
        if ( reader.hasNext() )
        {
            row = reader.next();
            return true;
        }
        else
        {
            row = null;
            return false;
        }
    }

    @Override
    public TimeUnit unit()
    {
        return unit;
    }

    @Override
    public String getOperationName()
    {
        assertRowNotNull( row );
        return row[INDEX_OPERATION_TYPE];
    }

    @Override
    public long getScheduledStartTimeAsMilli()
    {
        assertRowNotNull( row );
        return Long.parseLong( row[INDEX_SCHEDULED_START_TIME] );
    }

    @Override
    public long getActualStartTimeAsMilli()
    {
        assertRowNotNull( row );
        return Long.parseLong( row[INDEX_ACTUAL_START_TIME] );
    }

    @Override
    public long getRunDurationAsNano()
    {
        assertRowNotNull( row );
        return unit.toNanos( Long.parseLong( row[INDEX_EXECUTION_DURATION] ) );
    }

    @Override
    public int getResultCode()
    {
        assertRowNotNull( row );
        return Integer.parseInt( row[INDEX_RESULT_CODE] );
    }

    @Override
    public long getOriginalStartTime()
    {
        assertRowNotNull( row );
        return Long.parseLong( row[INDEX_ORIGINAL_START_TIME] );
    }

    private void assertRowNotNull( String[] row )
    {
        if ( null == row )
        {
            throw new RuntimeException( "Nothing to read. Reader has not been advanced or has reached EOF." );
        }
    }

    @Override
    public void close() throws Exception
    {
        reader.close();
    }
}
