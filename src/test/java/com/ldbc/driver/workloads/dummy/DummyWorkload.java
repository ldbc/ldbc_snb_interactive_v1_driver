package com.ldbc.driver.workloads.dummy;

import com.ldbc.driver.*;
import com.ldbc.driver.generator.GeneratorFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DummyWorkload extends Workload {
    private final long maxExpectedInterleaveAsMilli;
    private final WorkloadStreams workloadStreams;

    public DummyWorkload(WorkloadStreams workloadStreams,
                         long maxExpectedInterleaveAsMilli) {
        this.maxExpectedInterleaveAsMilli = maxExpectedInterleaveAsMilli;
        this.workloadStreams = workloadStreams;
    }

    @Override
    public Map<Integer, Class<? extends Operation<?>>> operationTypeToClassMapping(Map<String, String> params) {
        Map<Integer, Class<? extends Operation<?>>> operationTypeToClassMapping = new HashMap<>();
        operationTypeToClassMapping.put(NothingOperation.TYPE, NothingOperation.class);
        operationTypeToClassMapping.put(TimedNamedOperation1.TYPE, TimedNamedOperation1.class);
        operationTypeToClassMapping.put(TimedNamedOperation2.TYPE, TimedNamedOperation2.class);
        operationTypeToClassMapping.put(TimedNamedOperation3.TYPE, TimedNamedOperation3.class);
        return operationTypeToClassMapping;
    }

    @Override
    public void onInit(Map<String, String> params) throws WorkloadException {
    }

    @Override
    protected void onClose() throws IOException {
    }

    @Override
    protected WorkloadStreams getStreams(GeneratorFactory generators) throws WorkloadException {
        return newCopyOfWorkloadStreams();
    }

    private WorkloadStreams newCopyOfWorkloadStreams() {
        return workloadStreams;
    }

    @Override
    public String serializeOperation(Operation<?> operation) throws SerializingMarshallingException {
        if (operation.getClass().equals(NothingOperation.class)) return NothingOperation.class.getName();
        if (operation.getClass().equals(TimedNamedOperation1.class))
            return TimedNamedOperation1.class.getName()
                    + "|"
                    + Long.toString(operation.scheduledStartTimeAsMilli())
                    + "|"
                    + Long.toString(operation.timeStamp())
                    + "|"
                    + Long.toString(operation.dependencyTimeStamp())
                    + "|"
                    + serializeName(((TimedNamedOperation1) operation).name());
        if (operation.getClass().equals(TimedNamedOperation2.class))
            return TimedNamedOperation2.class.getName()
                    + "|"
                    + Long.toString(operation.scheduledStartTimeAsMilli())
                    + "|"
                    + Long.toString(operation.timeStamp())
                    + "|"
                    + Long.toString(operation.dependencyTimeStamp())
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
                    Long.parseLong(serializedOperationTokens[1]),
                    Long.parseLong(serializedOperationTokens[2]),
                    Long.parseLong(serializedOperationTokens[3]),
                    marshalName(serializedOperationTokens[4])
            );
        }
        if (serializedOperation.startsWith(TimedNamedOperation2.class.getName())) {
            String[] serializedOperationTokens = serializedOperation.split("\\|");
            return new TimedNamedOperation2(
                    Long.parseLong(serializedOperationTokens[1]),
                    Long.parseLong(serializedOperationTokens[2]),
                    Long.parseLong(serializedOperationTokens[3]),
                    marshalName(serializedOperationTokens[4])
            );
        }
        throw new SerializingMarshallingException("Unsupported Operation: " + serializedOperation);
    }

    private String serializeName(String name) {
        return (null == name) ? "null" : name;
    }

    private String marshalName(String nameString) {
        return ("null".equals(nameString)) ? null : nameString;
    }

    @Override
    public long maxExpectedInterleaveAsMilli() {
        return maxExpectedInterleaveAsMilli;
    }
}
