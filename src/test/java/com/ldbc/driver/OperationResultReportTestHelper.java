package com.ldbc.driver;

import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

public class OperationResultReportTestHelper {
    public static OperationResultReport create(int resultCode, Object operationResult) {
        return new OperationResultReport(resultCode, operationResult);
    }

    public static void setOperationType(OperationResultReport operationResultReport, String type) {
        operationResultReport.setOperationType(type);
    }

    public static void setScheduledStartTime(OperationResultReport operationResultReport, Time time) {
        operationResultReport.setScheduledStartTime(time);
    }

    public static void setActualStartTime(OperationResultReport operationResultReport, Time time) {
        operationResultReport.setActualStartTime(time);
    }

    public static void setRunDuration(OperationResultReport operationResultReport, Duration duration) {
        operationResultReport.setRunDuration(duration);
    }
}
