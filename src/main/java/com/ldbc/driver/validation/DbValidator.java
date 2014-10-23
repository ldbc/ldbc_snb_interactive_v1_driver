package com.ldbc.driver.validation;

import com.ldbc.driver.*;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.util.Tuple;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DbValidator {
    public DbValidationResult validate(Iterator<ValidationParam> validationParameters,
                                       Db db) throws WorkloadException {
        DbValidationResult dbValidationResult = new DbValidationResult(db);
        while (validationParameters.hasNext()) {
            ValidationParam validationParam = validationParameters.next();
            Operation<?> operation = validationParam.operation();
            Object expectedOperationResult = validationParam.operationResult();

            OperationHandler<Operation<?>> handler;
            try {
                handler = (OperationHandler<Operation<?>>) db.getOperationHandler(operation);
            } catch (DbException e) {
                dbValidationResult.reportMissingHandlerForOperation(operation);
                continue;
            }

            OperationResultReport actualOperationResultReport;
            try {
                actualOperationResultReport = handler.executeOperationUnsafe(operation);
            } catch (DbException e) {
                dbValidationResult.reportUnableToExecuteOperation(operation, ConcurrentErrorReporter.stackTraceToString(e));
                continue;
            } finally {
                handler.cleanup();
            }

            Object actualOperationResult = actualOperationResultReport.operationResult();

            if (false == expectedOperationResult.equals(actualOperationResult)) {
                dbValidationResult.reportIncorrectResultForOperation(operation, expectedOperationResult, actualOperationResult);
                continue;
            }

            dbValidationResult.reportSuccessfulExecution(operation);
        }
        return dbValidationResult;
    }
}