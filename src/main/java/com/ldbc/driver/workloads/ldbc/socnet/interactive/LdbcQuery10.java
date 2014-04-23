package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.List;

public class LdbcQuery10 extends Operation<List<LdbcQuery10Result>> {
    public static final int DEFAULT_LIMIT = 20;

    private final long personId;
    private final int horoscopeSign1;
    private final int horoscopeSign2;
    private final int limit;

    public LdbcQuery10(long personId, int horoscopeSign1, int horoscopeSign2, int limit) {
        this.personId = personId;
        this.horoscopeSign1 = horoscopeSign1;
        this.horoscopeSign2 = horoscopeSign2;
        this.limit = limit;
    }

    public long personId() {
        return personId;
    }

    public int horoscopeSign1() {
        return horoscopeSign1;
    }

    public int horoscopeSign2() {
        return horoscopeSign2;
    }

    public int limit() {
        return limit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery10 that = (LdbcQuery10) o;

        if (horoscopeSign1 != that.horoscopeSign1) return false;
        if (horoscopeSign2 != that.horoscopeSign2) return false;
        if (limit != that.limit) return false;
        if (personId != that.personId) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + horoscopeSign1;
        result = 31 * result + horoscopeSign2;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery10{" +
                "personId=" + personId +
                ", horoscopeSign1=" + horoscopeSign1 +
                ", horoscopeSign2=" + horoscopeSign2 +
                ", limit=" + limit +
                '}';
    }
}
