package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcQuery6Result {
    private final String tagName;
    private final int tagCount;

    public LdbcQuery6Result(String tagName, int tagCount) {
        this.tagName = tagName;
        this.tagCount = tagCount;
    }

    public String tagName() {
        return tagName;
    }

    public int tagCount() {
        return tagCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery6Result that = (LdbcQuery6Result) o;

        if (tagCount != that.tagCount) return false;
        if (tagName != null ? !tagName.equals(that.tagName) : that.tagName != null) return false;

        return true;
    }

    @Override
    public String toString() {
        return "LdbcQuery6Result{" +
                "tagName='" + tagName + '\'' +
                ", tagCount=" + tagCount +
                '}';
    }
}