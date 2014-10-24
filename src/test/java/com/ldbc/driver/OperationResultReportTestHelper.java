package com.ldbc.driver;

import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

public class OperationResultReportTestHelper {
    public static OperationResultReport create(int resultCode, Object operationResult, Operation<?> operation) {
        return new OperationResultReport(resultCode, operationResult, operation);
    }

    public static void setActualStartTime(OperationResultReport operationResultReport, Time time) {
        operationResultReport.setActualStartTimeAsMilli(time);
    }

    public static void setRunDuration(OperationResultReport operationResultReport, Duration duration) {
        operationResultReport.setRunDurationAsNano(duration);
    }
}
