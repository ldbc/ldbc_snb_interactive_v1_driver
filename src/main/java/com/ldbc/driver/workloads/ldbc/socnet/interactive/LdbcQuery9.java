package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.List;

public class LdbcQuery9 extends Operation<List<LdbcQuery9Result>> {
    private final long personId;
    private final long date;
    private final int limit;

    public LdbcQuery9(long personId, long date, int limit) {
        this.personId = personId;
        this.date = date;
        this.limit = limit;
    }

    public long personId() {
        return personId;
    }

    public long date() {
        return date;
    }

    public int limit() {
        return limit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery9 that = (LdbcQuery9) o;

        if (date != that.date) return false;
        if (limit != that.limit) return false;
        if (personId != that.personId) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (int) (date ^ (date >>> 32));
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery9{" +
                "personId=" + personId +
                ", date=" + date +
                ", limit=" + limit +
                '}';
    }
}
