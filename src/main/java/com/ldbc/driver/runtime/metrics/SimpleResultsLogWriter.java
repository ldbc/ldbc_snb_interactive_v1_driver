package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.csv.simple.SimpleCsvFileWriter;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

// TODO add tests for writer
// TODO add reader
// TODO add tests for reader
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
                "operation_type",
                "scheduled_start_time_" + TimeUnit.MILLISECONDS.name(),
                "actual_start_time_" + TimeUnit.MILLISECONDS.name(),
                "execution_duration_" + unit.name(),
                "result_code",
                "original_start_time"
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
