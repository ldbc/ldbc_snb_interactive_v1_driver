package com.ldbc.driver.testutils;

import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DummyWorkload extends Workload {
    private final List<Operation<?>> operations;
    private final Iterator<Operation<?>> alternativeLastOperations;
    private final Map<Class<? extends Operation>, OperationClassification> operationClassifications;
    private final Duration maxExpectedInterleave;

    public DummyWorkload(Iterator<Operation<?>> operations,
                         Map<Class<? extends Operation>, OperationClassification> operationClassifications,
                         Duration maxExpectedInterleave) {
        this(operations, null, operationClassifications, maxExpectedInterleave);
    }

    public DummyWorkload(Iterator<Operation<?>> operations,
                         Iterator<Operation<?>> alternativeLastOperations,
                         Map<Class<? extends Operation>, OperationClassification> operationClassifications,
                         Duration maxExpectedInterleave) {
        this.operations = Lists.newArrayList(operations);
        this.operationClassifications = operationClassifications;
        this.maxExpectedInterleave = maxExpectedInterleave;
        this.alternativeLastOperations = alternativeLastOperations;
    }


    @Override
    public Map<Class<? extends Operation>, OperationClassification> getOperationClassifications() {
        return operationClassifications;
    }

    @Override
    public void onInit(Map<String, String> params) throws WorkloadException {
    }

    @Override
    protected void onCleanup() throws WorkloadException {
    }

    @Override
    protected Iterator<Operation<?>> getOperations(GeneratorFactory generators) throws WorkloadException {
        if (null == alternativeLastOperations) {
            return operations.iterator();
        } else {
            List<Operation<?>> operationsToReturn = Lists.newArrayList(operations.iterator());
            operationsToReturn.remove(operationsToReturn.size() - 1);
            operationsToReturn.add(alternativeLastOperations.next());
            return operationsToReturn.iterator();
        }
    }

    @Override
    public String serializeOperation(Operation<?> operation) throws SerializingMarshallingException {
        if (operation.getClass().equals(NothingOperation.class)) return NothingOperation.class.getName();
        if (operation.getClass().equals(TimedOperation.class))
            return TimedOperation.class.getName()
                    + "|"
                    + serializeTime(operation.scheduledStartTime())
                    + "|"
                    + serializeTime(operation.dependencyTime());
        if (operation.getClass().equals(TimedNamedOperation.class))
            return TimedNamedOperation.class.getName()
                    + "|"
                    + serializeTime(operation.scheduledStartTime())
                    + "|"
                    + serializeTime(operation.dependencyTime())
                    + "|"
                    + ((TimedNamedOperation) operation).name();
        throw new SerializingMarshallingException("Unsupported Operation: " + operation.getClass().getName());
    }

    private String serializeTime(Time time) {
        return (null == time) ? "null" : Long.toString(time.asMilli());
    }

    private Time marshalTime(String timeString) {
        return ("null".equals(timeString)) ? null : Time.fromMilli(Long.parseLong(timeString));
    }

    @Override
    public Operation<?> marshalOperation(String serializedOperation) throws SerializingMarshallingException {
        if (serializedOperation.startsWith(NothingOperation.class.getName())) return new NothingOperation();
        if (serializedOperation.startsWith(TimedOperation.class.getName())) {
            String[] serializedOperationTokens = serializedOperation.split("\\|");
            return new TimedOperation(
                    marshalTime(serializedOperationTokens[1]),
                    marshalTime(serializedOperationTokens[2])
            );
        }
        if (serializedOperation.startsWith(TimedNamedOperation.class.getName())) {
            String[] serializedOperationTokens = serializedOperation.split("\\|");
            return new TimedNamedOperation(
                    marshalTime(serializedOperationTokens[1]),
                    marshalTime(serializedOperationTokens[2]),
                    serializedOperationTokens[3]
            );
        }
        throw new SerializingMarshallingException("Unsupported Operation: " + serializedOperation);
    }

    @Override
    public Duration maxExpectedInterleave() {
        return maxExpectedInterleave;
    }
}
