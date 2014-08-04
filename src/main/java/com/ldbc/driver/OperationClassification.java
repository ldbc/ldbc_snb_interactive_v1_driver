package com.ldbc.driver;

public class OperationClassification {

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

    public enum SchedulingMode {
        WINDOWED,
        INDIVIDUAL_BLOCKING,
        INDIVIDUAL_ASYNC
    }

    public enum DependencyMode {
        NONE,
        READ,
        READ_WRITE
    }

    private final SchedulingMode schedulingMode;
    private final DependencyMode dependencyMode;

    public OperationClassification(SchedulingMode schedulingMode, DependencyMode dependencyMode) {
        this.schedulingMode = schedulingMode;
        this.dependencyMode = dependencyMode;
    }

    public SchedulingMode schedulingMode() {
        return schedulingMode;
    }

    public DependencyMode dependencyMode() {
        return dependencyMode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OperationClassification that = (OperationClassification) o;

        if (dependencyMode != that.dependencyMode) return false;
        if (schedulingMode != that.schedulingMode) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = schedulingMode != null ? schedulingMode.hashCode() : 0;
        result = 31 * result + (dependencyMode != null ? dependencyMode.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "OperationClassification{" +
                "schedulingMode=" + schedulingMode +
                ", gctMode=" + dependencyMode +
                '}';
    }
}
