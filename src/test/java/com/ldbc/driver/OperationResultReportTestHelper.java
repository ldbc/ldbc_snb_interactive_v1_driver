package com.ldbc.driver;

public class OperationResultReportTestHelper {
    public static OperationResultReport create(int resultCode, Object operationResult, Operation<?> operation) {
        return new OperationResultReport(resultCode, operationResult, operation);
    }

    public static void setActualStartTime(OperationResultReport operationResultReport, long time) {
        operationResultReport.setActualStartTimeAsMilli(time);
    }

    public static void setRunDuration(OperationResultReport operationResultReport, long duration) {
        operationResultReport.setRunDurationAsNano(duration);
    }
}
