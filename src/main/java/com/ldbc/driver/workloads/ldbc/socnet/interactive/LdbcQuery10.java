package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.List;

public class LdbcQuery10 extends Operation<List<LdbcQuery10Result>> {
    public static final int DEFAULT_LIMIT = 20;

    private final long personId;
    private final int horoscopeMonth1;
    private final int horoscopeMonth2;
    private final int limit;

    public LdbcQuery10(long personId, int horoscopeMonth1, int horoscopeMonth2, int limit) {
        this.personId = personId;
        this.horoscopeMonth1 = horoscopeMonth1;
        this.horoscopeMonth2 = horoscopeMonth2;
        this.limit = limit;
    }

    public long personId() {
        return personId;
    }

    public int horoscopeMonth1() {
        return horoscopeMonth1;
    }

    public int horoscopeMonth2() {
        return horoscopeMonth2;
    }

    public int limit() {
        return limit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery10 that = (LdbcQuery10) o;

        if (horoscopeMonth1 != that.horoscopeMonth1) return false;
        if (horoscopeMonth2 != that.horoscopeMonth2) return false;
        if (limit != that.limit) return false;
        if (personId != that.personId) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + horoscopeMonth1;
        result = 31 * result + horoscopeMonth2;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery10{" +
                "personId=" + personId +
                ", horoscopeMonth1=" + horoscopeMonth1 +
                ", horoscopeMonth2=" + horoscopeMonth2 +
                ", limit=" + limit +
                '}';
    }
}
