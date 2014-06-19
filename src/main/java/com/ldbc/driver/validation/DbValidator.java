package com.ldbc.driver.validation;

import com.ldbc.driver.*;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.util.Tuple;

import java.util.Iterator;

// TODO test
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
    }

    public DbValidationResult validate(Iterator<Tuple.Tuple2<Operation<?>, Object>> operationsAndExpectedResults,
                                       Db db) {
        while (operationsAndExpectedResults.hasNext()) {
            Tuple.Tuple2<Operation<?>, Object> operationAndExpectedResult = operationsAndExpectedResults.next();
            Operation<?> operation = operationAndExpectedResult._1();
            Object expectedResult = operationAndExpectedResult._2();
            OperationHandler<Operation<?>> handler;
            try {
                handler = (OperationHandler<Operation<?>>) db.getOperationHandler(operation);
            } catch (DbException e) {
                // TODO maybe store all errors and return a log at the end, rather than returning after first error
                return new DbValidationResult(
                        false,
                        String.format("No operation handler found for operation\n%s\n%s",
                                operation, ConcurrentErrorReporter.stackTraceToString(e)));
            }
            OperationResult operationResult;
            try {
                operationResult = handler.executeUnsafe(operation);
            } catch (DbException e) {
                // TODO maybe store all errors and return a log at the end, rather than returning after first error
                return new DbValidationResult(
                        false,
                        String.format("Error encountered while trying to execute operation\n%s\n%s",
                                operation, ConcurrentErrorReporter.stackTraceToString(e)));
            }
            Object result = operationResult.result();
            if (false == result.equals(expectedResult))
                // TODO maybe store all errors and return a log at the end, rather than returning after first error
                return new DbValidationResult(
                        false,
                        String.format("Operation result not equal to expected result"));
        }

        return new DbValidationResult(true, null);
    }
}
