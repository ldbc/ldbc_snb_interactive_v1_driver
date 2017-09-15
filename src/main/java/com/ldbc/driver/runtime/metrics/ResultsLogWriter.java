package com.ldbc.driver.runtime.metrics;

import java.io.IOException;

public interface ResultsLogWriter extends AutoCloseable
{
    String HEADER_OPERATION_TYPE = "operation_type";
    String HEADER_SCHEDULED_START_TIME = "scheduled_start_time";
    String HEADER_ACTUAL_START_TIME = "actual_start_time";
    String HEADER_EXECUTION_DURATION_PREFIX = "execution_duration_";
    String HEADER_RESULT_CODE = "result_code";
    String HEADER_ORIGINAL_START_TIME = "original_start_time";

    int INDEX_OPERATION_TYPE = 0;
    int INDEX_SCHEDULED_START_TIME = 1;
    int INDEX_ACTUAL_START_TIME = 2;
    int INDEX_EXECUTION_DURATION = 3;
    int INDEX_RESULT_CODE = 4;
    int INDEX_ORIGINAL_START_TIME = 5;

    void write(
            String operationName,
            long scheduledStartTimeAsMilli,
            long actualStartTimeAsMilli,
            long runDurationAsNano,
            int resultCode,
            long originalStartTime ) throws IOException;
}
