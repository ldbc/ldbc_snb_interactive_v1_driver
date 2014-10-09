package com.ldbc.driver.workloads.dummy;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.*;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DummyWorkload extends Workload {
    private final Duration maxExpectedInterleave;
    private final Set<Class<? extends Operation<?>>> asynchronousDependentOperationTypes;
    private final List<Operation<?>> asynchronousDependencyOperations;
    private final List<Operation<?>> asynchronousNonDependencyOperations;
    private final List<Set<Class<? extends Operation<?>>>> blockingDependentOperationTypesList;
    private final List<List<Operation<?>>> blockingDependencyOperationsList;
    private final List<List<Operation<?>>> blockingNonDependencyOperationsList;

    public DummyWorkload(WorkloadStreams workloadStreams,
                         Duration maxExpectedInterleave) {
        this.maxExpectedInterleave = maxExpectedInterleave;

        asynchronousDependentOperationTypes = workloadStreams.asynchronousStream().dependentOperationTypes();
        asynchronousDependencyOperations = Lists.newArrayList(
                workloadStreams.asynchronousStream().dependencyOperations()
        );
        asynchronousNonDependencyOperations = Lists.newArrayList(
                workloadStreams.asynchronousStream().nonDependencyOperations()
        );
        blockingDependentOperationTypesList = Lists.newArrayList(
                Iterables.transform(
                        workloadStreams.blockingStreamDefinitions(),
                        new Function<WorkloadStreams.WorkloadStreamDefinition, Set<Class<? extends Operation<?>>>>() {
                            @Override
                            public Set<Class<? extends Operation<?>>> apply(WorkloadStreams.WorkloadStreamDefinition streamDefinition) {
                                return streamDefinition.dependentOperationTypes();
                            }
                        }
                )
        );
        blockingDependencyOperationsList = Lists.newArrayList(
                Iterables.transform(
                        workloadStreams.blockingStreamDefinitions(),
                        new Function<WorkloadStreams.WorkloadStreamDefinition, List<Operation<?>>>() {
                            @Override
                            public List<Operation<?>> apply(WorkloadStreams.WorkloadStreamDefinition streamDefinition) {
                                return Lists.newArrayList(streamDefinition.dependencyOperations());
                            }
                        }
                )
        );
        blockingNonDependencyOperationsList = Lists.newArrayList(
                Iterables.transform(
                        workloadStreams.blockingStreamDefinitions(),
                        new Function<WorkloadStreams.WorkloadStreamDefinition, List<Operation<?>>>() {
                            @Override
                            public List<Operation<?>> apply(WorkloadStreams.WorkloadStreamDefinition streamDefinition) {
                                return Lists.newArrayList(streamDefinition.nonDependencyOperations());
                            }
                        }
                )
        );
    }

    @Override
    public void onInit(Map<String, String> params) throws WorkloadException {
    }

    @Override
    protected void onCleanup() throws WorkloadException {
    }

    @Override
    protected WorkloadStreams getStreams(GeneratorFactory generators) throws WorkloadException {
        return newCopyOfWorkloadStreams();
    }

    private WorkloadStreams newCopyOfWorkloadStreams() {
        WorkloadStreams workloadStreams = new WorkloadStreams();

        workloadStreams.setAsynchronousStream(
                Sets.newHashSet(asynchronousDependentOperationTypes.iterator()),
                asynchronousDependencyOperations.iterator(),
                asynchronousNonDependencyOperations.iterator()
        );

        for (int i = 0; i < blockingDependentOperationTypesList.size(); i++) {
            workloadStreams.addBlockingStream(
                    Sets.newHashSet(blockingDependentOperationTypesList.get(i).iterator()),
                    blockingDependencyOperationsList.get(i).iterator(),
                    blockingNonDependencyOperationsList.get(i).iterator()
            );
        }

        return workloadStreams;
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
