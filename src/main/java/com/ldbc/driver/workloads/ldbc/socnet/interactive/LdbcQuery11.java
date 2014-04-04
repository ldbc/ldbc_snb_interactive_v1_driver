package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.List;

public class LdbcQuery11 extends Operation<List<LdbcQuery11Result>> {
    private final long personId;
    private final String country;
    private final long workFromDate;
    private final int limit;

    public LdbcQuery11(long personId, String country, long workFromDate, int limit) {
        this.personId = personId;
        this.country = country;
        this.workFromDate = workFromDate;
        this.limit = limit;
    }

    public long personId() {
        return personId;
    }

    public String country() {
        return country;
    }

    public long workFromDate() {
        return workFromDate;
    }

    public int limit() {
        return limit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery11 that = (LdbcQuery11) o;

        if (limit != that.limit) return false;
        if (personId != that.personId) return false;
        if (workFromDate != that.workFromDate) return false;
        if (country != null ? !country.equals(that.country) : that.country != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (country != null ? country.hashCode() : 0);
        result = 31 * result + (int) (workFromDate ^ (workFromDate >>> 32));
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery11{" +
                "personId=" + personId +
                ", country='" + country + '\'' +
                ", workFromDate=" + workFromDate +
                ", limit=" + limit +
                '}';
    }
}
