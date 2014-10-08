package com.ldbc.driver.workloads.dummy;

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
    private final Map<Class<? extends Operation>, OperationClassification> operationClassifications;
    private final Duration maxExpectedInterleave;

    public DummyWorkload(Iterator<Operation<?>> operations,
                         Map<Class<? extends Operation>, OperationClassification> operationClassifications,
                         Duration maxExpectedInterleave) {
        this.operations = Lists.newArrayList(operations);
        this.operationClassifications = operationClassifications;
        this.maxExpectedInterleave = maxExpectedInterleave;
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
    protected Iterator<Operation<?>> getStreams(GeneratorFactory generators) throws WorkloadException {
        return operations.iterator();
    }

    @Override
    public String serializeOperation(Operation<?> operation) throws SerializingMarshallingException {
        if (operation.getClass().equals(NothingOperation.class)) return NothingOperation.class.getName();
        if (operation.getClass().equals(TimedNamedOperation1.class))
            return TimedNamedOperation1.class.getName()
                    + "|"
                    + serializeTime(operation.scheduledStartTime())
                    + "|"
                    + serializeTime(operation.dependencyTime())
                    + "|"
                    + serializeName(((TimedNamedOperation1) operation).name());
        if (operation.getClass().equals(TimedNamedOperation2.class))
            return TimedNamedOperation2.class.getName()
                    + "|"
                    + serializeTime(operation.scheduledStartTime())
                    + "|"
                    + serializeTime(operation.dependencyTime())
                    + "|"
                    + serializeName(((TimedNamedOperation2) operation).name());
        throw new SerializingMarshallingException("Unsupported Operation: " + operation.getClass().getName());
    }

    @Override
    public Operation<?> marshalOperation(String serializedOperation) throws SerializingMarshallingException {
        if (serializedOperation.startsWith(NothingOperation.class.getName())) return new NothingOperation();
        if (serializedOperation.startsWith(TimedNamedOperation1.class.getName())) {
            String[] serializedOperationTokens = serializedOperation.split("\\|");
            return new TimedNamedOperation1(
                    marshalTime(serializedOperationTokens[1]),
                    marshalTime(serializedOperationTokens[2]),
                    marshalName(serializedOperationTokens[3])
            );
        }
        if (serializedOperation.startsWith(TimedNamedOperation2.class.getName())) {
            String[] serializedOperationTokens = serializedOperation.split("\\|");
            return new TimedNamedOperation2(
                    marshalTime(serializedOperationTokens[1]),
                    marshalTime(serializedOperationTokens[2]),
                    marshalName(serializedOperationTokens[3])
            );
        }
        throw new SerializingMarshallingException("Unsupported Operation: " + serializedOperation);
    }

    private String serializeTime(Time time) {
        return (null == time) ? "null" : Long.toString(time.asMilli());
    }

    private Time marshalTime(String timeString) {
        return ("null".equals(timeString)) ? null : Time.fromMilli(Long.parseLong(timeString));
    }

    private String serializeName(String name) {
        return (null == name) ? "null" : name;
    }

    private String marshalName(String nameString) {
        return ("null".equals(nameString)) ? null : nameString;
    }

    @Override
    public Duration maxExpectedInterleave() {
        return maxExpectedInterleave;
    }
}
