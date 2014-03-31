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

    public enum GctMode {
        NONE,
        READ,
        READ_WRITE
    }

    private final SchedulingMode schedulingMode;
    private final GctMode gctMode;

    public OperationClassification(SchedulingMode schedulingMode, GctMode gctMode) {
        this.schedulingMode = schedulingMode;
        this.gctMode = gctMode;
    }

    public SchedulingMode schedulingMode() {
        return schedulingMode;
    }

    public GctMode gctMode() {
        return gctMode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OperationClassification that = (OperationClassification) o;

        if (gctMode != that.gctMode) return false;
        if (schedulingMode != that.schedulingMode) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = schedulingMode != null ? schedulingMode.hashCode() : 0;
        result = 31 * result + (gctMode != null ? gctMode.hashCode() : 0);
        return result;
    }
}
