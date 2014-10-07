package com.ldbc.driver;

import java.util.*;

public class WorkloadStreams {
    /**
     * Modes (with examples from LDBC Interactive SNB Workload):
     * - WINDOWED & NONE -------------------> n/a
     * - WINDOWED & READ -------------------> Create Friendship
     * - WINDOWED & READ WRITE -------------> Create User
     * - INDIVIDUAL_BLOCKING & NONE --------> n/a
     * - INDIVIDUAL_BLOCKING & READ --------> Create Post
     * - INDIVIDUAL_BLOCKING & READ WRITE --> n/a
     * - INDIVIDUAL_ASYNC & NONE -----------> Entire Read Workload
     * - INDIVIDUAL_ASYNC & READ -----------> n/a
     * - INDIVIDUAL_ASYNC & READ WRITE -----> n/a
     */
    public enum DependencyMode {
        NONE,
        READ,
        READ_WRITE
    }

    private AsynchronousStreamDefinition windowedOperationStreamDefinition = null;
    private AsynchronousStreamDefinition asynchronousOperationStreamDefinition = null;
    private List<AsynchronousStreamDefinition> synchronousOperationStreamDefinitions = new ArrayList<>();

    public AsynchronousStreamDefinition windowedOperationStreamDefinition() {
        return windowedOperationStreamDefinition;
    }

    public void setWindowedOperationStreamDefinition(AsynchronousStreamDefinition windowedOperationStreamDefinition) {
        this.windowedOperationStreamDefinition = windowedOperationStreamDefinition;
    }

    public AsynchronousStreamDefinition asynchronousOperationStreamDefinition() {
        return asynchronousOperationStreamDefinition;
    }

    public void setAsynchronousOperationStreamDefinition(AsynchronousStreamDefinition asynchronousOperationStreamDefinition) {
        this.asynchronousOperationStreamDefinition = asynchronousOperationStreamDefinition;
    }

    public List<AsynchronousStreamDefinition> synchronousOperationStreamDefinitions() {
        return synchronousOperationStreamDefinitions;
    }

    public void addSynchronousOperationStream(AsynchronousStreamDefinition synchronousOperationStream) {
        this.synchronousOperationStreamDefinitions.add(synchronousOperationStream);
    }

    public class AsynchronousStreamDefinition {
        private final Map<Class<? extends Operation>, DependencyMode> operationClassifications;
        private final Iterator<Operation<?>> operationStream;

        public AsynchronousStreamDefinition(Map<Class<? extends Operation>, DependencyMode> operationClassifications, Iterator<Operation<?>> operationStream) {
            this.operationClassifications = operationClassifications;
            this.operationStream = operationStream;
        }

        public Map<Class<? extends Operation>, DependencyMode> operationClassifications() {
            return operationClassifications;
        }

        public Iterator<Operation<?>> operations() {
            return operationStream;
        }

        public Set<Class<? extends Operation>> operationTypes() {
            return operationClassifications.keySet();
        }

        public boolean containsReadDependencies() {
            for (DependencyMode dependencyMode : operationClassifications.values()) {
                if (dependencyMode.equals(DependencyMode.READ)) return true;
            }
            return false;
        }

        public boolean containsWriteDependencies() {
            for (DependencyMode dependencyMode : operationClassifications.values()) {
                if (dependencyMode.equals(DependencyMode.READ_WRITE)) return true;
            }
            return false;
        }
    }

    public class SynchronousStreamDefinition {
        private final Map<Class<? extends Operation>, DependencyMode> operationClassifications;
        private final Iterator<Operation<?>> operationStream;

        public SynchronousStreamDefinition(Map<Class<? extends Operation>, DependencyMode> operationClassifications, Iterator<Operation<?>> operationStream) {
            this.operationClassifications = operationClassifications;
            this.operationStream = operationStream;
        }

        public Map<Class<? extends Operation>, DependencyMode> operationClassifications() {
            return operationClassifications;
        }

        public Iterator<Operation<?>> operations() {
            return operationStream;
        }

        public Set<Class<? extends Operation>> operationTypes() {
            return operationClassifications.keySet();
        }

        public boolean containsReadDependencies() {
            for (DependencyMode dependencyMode : operationClassifications.values()) {
                if (dependencyMode.equals(DependencyMode.READ)) return true;
            }
            return false;
        }

        public boolean containsWriteDependencies() {
            for (DependencyMode dependencyMode : operationClassifications.values()) {
                if (dependencyMode.equals(DependencyMode.READ_WRITE)) return true;
            }
            return false;
        }
    }}
