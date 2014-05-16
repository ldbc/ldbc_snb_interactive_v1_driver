package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.List;

public class LdbcQuery14 extends Operation<List<LdbcQuery14Result>> {
    public static final int DEFAULT_LIMIT = 10;

    private final long personId1;
    private final long personId2;
    private final int limit;

    public LdbcQuery14(long personId1, long personId2, int limit) {
        this.personId1 = personId1;
        this.personId2 = personId2;
        this.limit = limit;
    }

    public long personId1() {
        return personId1;
    }

    public long personId2() {
        return personId2;
    }

    public int limit() {
        return limit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery14 that = (LdbcQuery14) o;

        if (limit != that.limit) return false;
        if (personId1 != that.personId1) return false;
        if (personId2 != that.personId2) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId1 ^ (personId1 >>> 32));
        result = 31 * result + (int) (personId2 ^ (personId2 >>> 32));
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery14{" +
                "personId1=" + personId1 +
                ", personId2=" + personId2 +
                ", limit=" + limit +
                '}';
    }
}
