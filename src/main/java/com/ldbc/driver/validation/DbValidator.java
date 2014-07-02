package com.ldbc.driver.validation;

import com.ldbc.driver.*;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;

import java.util.Iterator;

public class DbValidator {
    public static class DbValidationResult {
        private final boolean successful;
        private final String errorMessage;

        public DbValidationResult(boolean successful, String errorMessage) {
            this.successful = successful;
            this.errorMessage = errorMessage;
        }

        public boolean isSuccessful() {
            return successful;
        }

        public String errorMessage() {
            return errorMessage;
        }

        @Override
        public String toString() {
            return "DbValidationResult{" +
                    "successful=" + successful +
                    ", errorMessage='" + errorMessage + '\'' +
                    '}';
        }
    }

    public DbValidationResult validate(Iterator<ValidationParam> validationParameters,
                                       Db db) throws WorkloadException {
        while (validationParameters.hasNext()) {
            ValidationParam validationParam = validationParameters.next();
            Operation<?> operation = validationParam.operation();
            Object expectedOperationResult = validationParam.operationResult();

            OperationHandler<Operation<?>> handler;
            try {
                handler = (OperationHandler<Operation<?>>) db.getOperationHandler(operation);
            } catch (DbException e) {
                // TODO maybe store all errors and return a log at the end, rather than returning after first error
                return new DbValidationResult(
                        false,
                        String.format(""
                                + "No operation handler found for operation\n"
                                + "Db: %s\n"
                                + "Operation: %s\n"
                                + "%s",
                                db.getClass().getName(), operation, ConcurrentErrorReporter.stackTraceToString(e)));
            }

            OperationResultReport actualOperationResultReport;
            try {
                actualOperationResultReport = handler.executeUnsafe(operation);
            } catch (DbException e) {
                // TODO maybe store all errors and return a log at the end, rather than returning after first error
                return new DbValidationResult(
                        false,
                        String.format(""
                                + "Error encountered while trying to execute operation\n"
                                + "Db: %s\n"
                                + "Operation: %s\n"
                                + "%s",
                                db.getClass().getName(), operation, ConcurrentErrorReporter.stackTraceToString(e)));
            }

            Object actualOperationResult = actualOperationResultReport.operationResult();

            if (false == expectedOperationResult.equals(actualOperationResult)) {
                // TODO maybe store all errors and return a log at the end, rather than returning after first error
                return new DbValidator.DbValidationResult(
                        false,
                        String.format(""
                                + "Invalid operation result\n"
                                + "Operation: %s\n"
                                + "Expected Result: %s\n"
                                + "Actual Result: %s",
                                operation, expectedOperationResult, actualOperationResult));
            }
        }

        return new DbValidator.DbValidationResult(true, null);
    }
}