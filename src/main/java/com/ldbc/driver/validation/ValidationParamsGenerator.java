package com.ldbc.driver.validation;

import com.ldbc.driver.*;
import com.ldbc.driver.Workload.DbValidationParametersFilter;
import com.ldbc.driver.Workload.DbValidationParametersFilterResult;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ValidationParamsGenerator extends Generator<ValidationParam> {
    private final Db db;
    private final DbValidationParametersFilter dbValidationParametersFilter;
    private final Iterator<Operation> operations;
    private final ResultReporter resultReporter = new ResultReporter.SimpleResultReporter();
    private int entriesWrittenSoFar;
    private boolean needMoreValidationParameters;
    private final List<Operation> injectedOperations;

    public ValidationParamsGenerator(Db db,
                                     DbValidationParametersFilter dbValidationParametersFilter,
                                     Iterator<Operation> operations) {
        this.db = db;
        this.dbValidationParametersFilter = dbValidationParametersFilter;
        this.operations = operations;
        this.entriesWrittenSoFar = 0;
        this.needMoreValidationParameters = true;
        this.injectedOperations = new ArrayList<>();
    }

    public int entriesWrittenSoFar() {
        return entriesWrittenSoFar;
    }

    @Override
    protected ValidationParam doNext() throws GeneratorException {
        while ((injectedOperations.size() > 0 || operations.hasNext()) && needMoreValidationParameters) {
            Operation operation;
            if (injectedOperations.isEmpty()) {
                operation = operations.next();
            } else {
                operation = injectedOperations.remove(0);
            }

            if (false == dbValidationParametersFilter.useOperation(operation))
                continue;

            OperationHandlerRunnableContext operationHandlerRunner;
            try {
                operationHandlerRunner = db.getOperationHandlerRunnableContext(operation);
            } catch (DbException e) {
                throw new GeneratorException(
                        String.format(""
                                        + "Error retrieving operation handler for operation\n"
                                        + "Db: %s\n"
                                        + "Operation: %s",
                                db.getClass().getName(), operation),
                        e);
            }
            try {
                OperationHandler operationHandler = operationHandlerRunner.operationHandler();
                DbConnectionState dbConnectionState = operationHandlerRunner.dbConnectionState();
                operationHandler.executeOperation(operation, dbConnectionState, resultReporter);
            } catch (DbException e) {
                throw new GeneratorException(
                        String.format(""
                                        + "Error executing operation to retrieve validation result\n"
                                        + "Db: %s\n"
                                        + "Operation: %s",
                                db.getClass().getName(), operation),
                        e);
            } finally {
                operationHandlerRunner.cleanup();
            }

            Object result = resultReporter.result();
            DbValidationParametersFilterResult dbValidationParametersFilterResult = dbValidationParametersFilter.useOperationAndResultForValidation(operation, result);
            injectedOperations.addAll(dbValidationParametersFilterResult.injectedOperations());

            switch (dbValidationParametersFilterResult.acceptance()) {
                case REJECT_AND_CONTINUE:
                    continue;
                case REJECT_AND_FINISH:
                    needMoreValidationParameters = false;
                    continue;
                case ACCEPT_AND_CONTINUE:
                    entriesWrittenSoFar++;
                    return ValidationParam.createUntyped(operation, result);
                case ACCEPT_AND_FINISH:
                    entriesWrittenSoFar++;
                    needMoreValidationParameters = false;
                    return ValidationParam.createUntyped(operation, result);
                default:
                    throw new GeneratorException(
                            String.format("Unrecognized %s value: %s",
                                    Workload.DbValidationParametersFilterAcceptance.class.getSimpleName(),
                                    dbValidationParametersFilterResult.acceptance().name()
                            )
                    );
            }
        }
        // ran out of operations OR validation set size has been reached
        return null;
    }
}