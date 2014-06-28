package com.ldbc.driver.validation;

import com.ldbc.driver.*;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;

import java.util.Iterator;

// TODO test
public class ValidationParamsGenerator extends Generator<ValidationParam> {
    private final Db db;
    private final Workload workload;
    private final Iterator<Operation<?>> operations;
    private final int validationSetSize;
    private int entriesWrittenSoFar;

    public ValidationParamsGenerator(Db db,
                                     Workload workload,
                                     Iterator<Operation<?>> operations,
                                     int validationSetSize) {
        this.db = db;
        this.workload = workload;
        this.operations = operations;
        this.validationSetSize = validationSetSize;
        this.entriesWrittenSoFar = 0;
    }

    public int entriesWrittenSoFar() {
        return entriesWrittenSoFar;
    }

    @Override
    protected ValidationParam doNext() throws GeneratorException {
        while (operations.hasNext() && entriesWrittenSoFar < validationSetSize) {
            Operation<?> operation = operations.next();

            OperationHandler<Operation<?>> handler;
            try {
                handler = (OperationHandler<Operation<?>>) db.getOperationHandler(operation);
            } catch (DbException e) {
                throw new GeneratorException(
                        String.format(""
                                + "Error retrieving operation handler for operation\n"
                                + "Db: %s\n"
                                + "Operation: %s",
                                db.getClass().getName(), operation),
                        e);
            }
            OperationResultReport operationResultReport;
            try {
                operationResultReport = handler.executeUnsafe(operation);
            } catch (DbException e) {
                throw new GeneratorException(
                        String.format(""
                                + "Error executing operation to retrieve validation result\n"
                                + "Db: %s\n"
                                + "Operation: %s",
                                db.getClass().getName(), operation),
                        e);
            }
            Object operationResult = operationResultReport.operationResult();

            if (false == workload.validationResultCheck(operation, operationResult))
                // operation/result not suitable for use in validation set
                continue;

            entriesWrittenSoFar++;
            return new ValidationParam(operation, operationResult);
        }
        // ran out of operations OR validation set size has been reached
        return null;
    }
}