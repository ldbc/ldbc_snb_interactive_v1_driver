package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.List;

public class LdbcQuery13 extends Operation<List<LdbcQuery13Result>> {
    private final long personId1;
    private final long personId2;

    public LdbcQuery13(long personId1, long personId2) {
        this.personId1 = personId1;
        this.personId2 = personId2;
    }

    public long personId1() {
        return personId1;
    }

    public long personId2() {
        return personId2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery13 that = (LdbcQuery13) o;

        if (personId1 != that.personId1) return false;
        if (personId2 != that.personId2) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId1 ^ (personId1 >>> 32));
        result = 31 * result + (int) (personId2 ^ (personId2 >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery13{" +
                "personId1=" + personId1 +
                ", personId2=" + personId2 +
                '}';
    }
}
