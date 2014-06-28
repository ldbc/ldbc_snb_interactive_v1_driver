package com.ldbc.driver.validation;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.Workload;
import com.ldbc.driver.generator.GeneratorException;

import java.util.Iterator;

public class ValidationParamsToCsvRows implements Iterator<String[]> {
    private final Iterator<ValidationParam> validationParams;
    private final Workload workload;
    private final boolean performSerializationMarshallingChecks;

    public ValidationParamsToCsvRows(Iterator<ValidationParam> validationParams,
                                     Workload workload,
                                     boolean performSerializationMarshallingChecks) {
        this.validationParams = validationParams;
        this.workload = workload;
        this.performSerializationMarshallingChecks = performSerializationMarshallingChecks;
    }

    @Override
    public boolean hasNext() {
        return validationParams.hasNext();
    }

    @Override
    public String[] next() {
        ValidationParam validationParam = validationParams.next();
        Operation<?> operation = validationParam.operation();
        Object operationResult = validationParam.operationResult();

        String serializedOperation;
        try {
            serializedOperation = workload.serializeOperation(operation);
        } catch (SerializingMarshallingException e) {
            throw new GeneratorException(
                    String.format(""
                            + "Workload(%s) unable to serialize operation\n"
                            + "Operation: %s",
                            operation),
                    e);
        }

        String serializedOperationResult;
        try {
            serializedOperationResult = operation.serializeResult(operationResult);
        } catch (SerializingMarshallingException e) {
            throw new GeneratorException(
                    String.format(""
                            + "Error serializing operation result\n"
                            + "Operation: %s\n"
                            + "Operation Result: %s",
                            operation, operationResult),
                    e);
        }

        // Assert that serialization/marshalling is performed correctly
        if (performSerializationMarshallingChecks) {
            Object marshaledOperationResult;
            try {
                marshaledOperationResult = operation.marshalResult(serializedOperationResult);
            } catch (SerializingMarshallingException e) {
                throw new GeneratorException(
                        String.format(""
                                + "Error marshalling serialized operation result\n"
                                + "Operation: %s\n"
                                + "Operation Result: %s\n"
                                + "Serialized Result: %s",
                                operation, operationResult, serializedOperationResult),
                        e);
            }
            if (false == marshaledOperationResult.equals(operationResult))
                throw new GeneratorException(
                        String.format(""
                                + "Operation result and serialized-then-marshaled operation result do not equal\n"
                                + "Original: %s\n"
                                + "Serialized: %s\n"
                                + "Marshaled: %s",
                                operationResult, serializedOperationResult, marshaledOperationResult)
                );
        }

        return new String[]{serializedOperation, serializedOperationResult};
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove() not supported by " + getClass().getName());
    }
}