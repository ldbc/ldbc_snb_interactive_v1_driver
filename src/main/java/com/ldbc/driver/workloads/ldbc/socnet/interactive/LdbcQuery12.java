package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.List;

public class LdbcQuery12 extends Operation<List<LdbcQuery12Result>> {
    private final long personId;
    private final String tagClass;
    private final int limit;

    public LdbcQuery12(long personId, String tagClass, int limit) {
        this.personId = personId;
        this.tagClass = tagClass;
        this.limit = limit;
    }

    public long personId() {
        return personId;
    }

    public String tagClass() {
        return tagClass;
    }

    public int limit() {
        return limit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery12 that = (LdbcQuery12) o;

        if (limit != that.limit) return false;
        if (personId != that.personId) return false;
        if (tagClass != null ? !tagClass.equals(that.tagClass) : that.tagClass != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (tagClass != null ? tagClass.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery12{" +
                "personId=" + personId +
                ", tagClass='" + tagClass + '\'' +
                ", limit=" + limit +
                '}';
    }
}
