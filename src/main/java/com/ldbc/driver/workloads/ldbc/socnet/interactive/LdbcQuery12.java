package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.List;

public class LdbcQuery12 extends Operation<List<LdbcQuery12Result>> {
    public static final int DEFAULT_LIMIT = 20;

    private final long personId;
    private final long tagClassId;
    private final int limit;

    public LdbcQuery12(long personId, long tagClassId, int limit) {
        this.personId = personId;
        this.tagClassId = tagClassId;
        this.limit = limit;
    }

    public long personId() {
        return personId;
    }

    public long tagClassId() {
        return tagClassId;
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
        if (tagClassId != that.tagClassId) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (int) (tagClassId ^ (tagClassId >>> 32));
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery12{" +
                "personId=" + personId +
                ", tagClassId=" + tagClassId +
                ", limit=" + limit +
                '}';
    }
}
