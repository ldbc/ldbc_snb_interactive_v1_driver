package com.ldbc.driver.validation;

import com.ldbc.driver.*;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;

import java.text.DecimalFormat;
import java.util.Iterator;

public class DbValidator {
    public DbValidationResult validate(Iterator<ValidationParam> validationParameters,
                                       Db db,
                                       int validationParamsCount) throws WorkloadException {
        System.out.println("----");
        DecimalFormat numberFormat = new DecimalFormat("###,###,###,###,###");
        DbValidationResult dbValidationResult = new DbValidationResult(db);
        ResultReporter resultReporter = new ResultReporter.SimpleResultReporter();
        int validationParamsCompletedSoFar = 0;
        while (validationParameters.hasNext()) {
            ValidationParam validationParam = validationParameters.next();
            Operation<?> operation = validationParam.operation();
            Object expectedOperationResult = validationParam.operationResult();

            OperationHandlerRunnableContext handlerRunner;
            try {
                handlerRunner = db.getOperationHandlerRunnableContext(operation);
            } catch (DbException e) {
                dbValidationResult.reportMissingHandlerForOperation(operation);
                continue;
            }

            try {
                OperationHandler handler = handlerRunner.operationHandler();
                DbConnectionState dbConnectionState = handlerRunner.dbConnectionState();
                System.out.print(String.format("Validated %s / %s - currently executing: %s...\r",
                        numberFormat.format(validationParamsCompletedSoFar),
                        numberFormat.format(validationParamsCount),
                        operation.getClass().getSimpleName()
                ));
                handler.executeOperation(operation, dbConnectionState, resultReporter);
            } catch (DbException e) {
                dbValidationResult.reportUnableToExecuteOperation(operation, ConcurrentErrorReporter.stackTraceToString(e));
                continue;
            } finally {
                validationParamsCompletedSoFar++;
                handlerRunner.cleanup();
            }

            Object actualOperationResult = resultReporter.result();

            if (false == expectedOperationResult.equals(actualOperationResult)) {
                dbValidationResult.reportIncorrectResultForOperation(operation, expectedOperationResult, actualOperationResult);
                continue;
            }

            dbValidationResult.reportSuccessfulExecution(operation);
        }
        System.out.println("----");
        return dbValidationResult;
    }
}