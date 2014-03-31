package com.ldbc.driver;

public class OperationClassification {
    public enum SchedulingMode {
        WINDOWED,
        INDIVIDUAL_SYNC,
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
}
