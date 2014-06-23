package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcQuery6Result {
    private final String tagName;
    private final long tagCount;

    public LdbcQuery6Result(String tagName, long tagCount) {
        super();
        this.tagName = tagName;
        this.tagCount = tagCount;
    }

    public String tagName() {
        return tagName;
    }

    public long tagCount() {
        return tagCount;
    }

    @Override
    public String toString() {
        return "LdbcQuery6Result{" +
                "tagName='" + tagName + '\'' +
                ", tagCount=" + tagCount +
                '}';
    }
}