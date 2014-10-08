package com.ldbc.driver;

import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.temporal.Time;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class WorkloadStreams {
    private WorkloadStreamDefinition asynchronousStream = null;
    private List<WorkloadStreamDefinition> blockingStreams = new ArrayList<>();

    public WorkloadStreamDefinition asynchronousStream() {
        return asynchronousStream;
    }

    public void setAsynchronousStream(Set<Class<? extends Operation<?>>> dependentOperationTypes,
                                      Iterator<Operation<?>> dependencyOperations,
                                      Iterator<Operation<?>> nonDependencyOperations) {
        this.asynchronousStream = new WorkloadStreamDefinition(dependentOperationTypes, dependencyOperations, nonDependencyOperations);
    }

    public List<WorkloadStreamDefinition> blockingStreamDefinitions() {
        return blockingStreams;
    }

    public void addBlockingStream(Set<Class<? extends Operation<?>>> dependentOperationTypes,
                                  Iterator<Operation<?>> dependencyOperations,
                                  Iterator<Operation<?>> nonDependencyOperations) {
        WorkloadStreamDefinition blockingStream = new WorkloadStreamDefinition(dependentOperationTypes, dependencyOperations, nonDependencyOperations);
        this.blockingStreams.add(blockingStream);
    }

    public WorkloadStreams applyTimeOffsetAndCompressionRatio(GeneratorFactory gf, Time newStartTime, double timeCompressionRatio) {
        // TODO test
        // TODO actually apply time shift logic
        WorkloadStreams workloadStreams = new WorkloadStreams();
        for (WorkloadStreamDefinition streamDefinition : blockingStreamDefinitions()) {
            workloadStreams.addBlockingStream(
                    streamDefinition.dependentOperationTypes(),
                    streamDefinition.dependencyOperations(),
                    streamDefinition.nonDependencyOperations()
            );
        }
        workloadStreams.setAsynchronousStream(
                asynchronousStream().dependentOperationTypes(),
                asynchronousStream().dependencyOperations(),
                asynchronousStream().nonDependencyOperations()
        );
        return workloadStreams;
    }

    public Iterator<Operation<?>> mergeSortedByStartTime(GeneratorFactory gf) {
        // TODO test
        List<Iterator<Operation<?>>> allStreams = new ArrayList<>();
        for (WorkloadStreamDefinition streamDefinition : blockingStreamDefinitions()) {
            allStreams.add(streamDefinition.dependencyOperations());
            allStreams.add(streamDefinition.nonDependencyOperations());
        }
        allStreams.add(asynchronousStream().dependencyOperations());
        allStreams.add(asynchronousStream().nonDependencyOperations());
        return gf.mergeSortOperationsByStartTime(allStreams.toArray(new Iterator[allStreams.size()]));
    }

    public static class WorkloadStreamDefinition {
        private final Set<Class<? extends Operation<?>>> dependentOperationTypes;
        private final Iterator<Operation<?>> dependencyOperations;
        private final Iterator<Operation<?>> nonDependencyOperations;

        public WorkloadStreamDefinition(Set<Class<? extends Operation<?>>> dependentOperationTypes,
                                        Iterator<Operation<?>> dependencyOperations,
                                        Iterator<Operation<?>> nonDependencyOperations) {
            this.dependentOperationTypes = dependentOperationTypes;
            this.dependencyOperations = dependencyOperations;
            this.nonDependencyOperations = nonDependencyOperations;
        }

        public Iterator<Operation<?>> dependencyOperations() {
            return dependencyOperations;
        }

        public Iterator<Operation<?>> nonDependencyOperations() {
            return nonDependencyOperations;
        }

        public Set<Class<? extends Operation<?>>> dependentOperationTypes() {
            return dependentOperationTypes;
        }
    }
}
