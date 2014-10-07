package com.ldbc.driver;

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

    public void setAsynchronousStream(Set<Class<? extends Operation>> dependentOperationTypes,
                                      Iterator<Operation<?>> dependencyOperations,
                                      Iterator<Operation<?>> nonDependencyOperations) {
        this.asynchronousStream = new WorkloadStreamDefinition(dependentOperationTypes, dependencyOperations, nonDependencyOperations);
    }

    public List<WorkloadStreamDefinition> blockingStreamDefinitions() {
        return blockingStreams;
    }

    public void addBlockingStream(Set<Class<? extends Operation>> dependentOperationTypes,
                                     Iterator<Operation<?>> dependencyOperations,
                                     Iterator<Operation<?>> nonDependencyOperations) {
        WorkloadStreamDefinition blockingStream = new WorkloadStreamDefinition(dependentOperationTypes, dependencyOperations, nonDependencyOperations);
        this.blockingStreams.add(blockingStream);
    }


    public static class WorkloadStreamDefinition {
        private final Set<Class<? extends Operation>> dependentOperationTypes;
        private final Iterator<Operation<?>> dependencyOperations;
        private final Iterator<Operation<?>> nonDependencyOperations;

        public WorkloadStreamDefinition(Set<Class<? extends Operation>> dependentOperationTypes,
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

        public boolean isDependentOperation(Operation<?> operation) {
            return dependentOperationTypes.contains(operation.getClass());
        }
    }
}
