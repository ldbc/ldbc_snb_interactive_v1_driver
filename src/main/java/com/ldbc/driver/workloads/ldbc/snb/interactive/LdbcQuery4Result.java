package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcQuery4Result {
    private final String tagName;
    private final int tagCount;

    public LdbcQuery4Result(String tagName, int tagCount) {
        super();
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

        LdbcQuery4Result that = (LdbcQuery4Result) o;

        if (tagCount != that.tagCount) return false;
        if (tagName != null ? !tagName.equals(that.tagName) : that.tagName != null) return false;

        return true;
    }

    @Override
    public String toString() {
        return "LdbcQuery4Result{" +
                "tagName='" + tagName + '\'' +
                ", tagCount=" + tagCount +
                '}';
    }
}