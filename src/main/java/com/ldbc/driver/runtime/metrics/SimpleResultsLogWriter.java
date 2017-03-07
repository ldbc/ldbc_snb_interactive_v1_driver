package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.csv.simple.SimpleCsvFileWriter;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class SimpleResultsLogWriter implements ResultsLogWriter
{
    private final SimpleCsvFileWriter writer;
    private final TimeUnit unit;

    public SimpleResultsLogWriter( File resultsLog, TimeUnit unit ) throws IOException
    {
        this.writer = new SimpleCsvFileWriter( resultsLog, SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR );
        this.unit = unit;
        resultsLog.createNewFile();
        writer.writeRow(
                HEADER_OPERATION_TYPE,
                HEADER_SCHEDULED_START_TIME,
                HEADER_ACTUAL_START_TIME,
                HEADER_EXECUTION_DURATION_PREFIX + unit.name(),
                HEADER_RESULT_CODE,
                HEADER_ORIGINAL_START_TIME
        );
    }

    @Override
    public void write(
            String operationName,
            long scheduledStartTimeAsMilli,
            long actualStartTimeAsMilli,
            long runDurationAsNano,
            int resultCode,
            long originalStartTime ) throws IOException
    {
        writer.writeRow(
                operationName,
                Long.toString( scheduledStartTimeAsMilli ),
                Long.toString( actualStartTimeAsMilli ),
                Long.toString( unit.convert( runDurationAsNano, TimeUnit.NANOSECONDS ) ),
                Integer.toString( resultCode ),
                Long.toString( originalStartTime )
        );
    }

    @Override
    public void close() throws Exception
    {
        writer.close();
    }
}
