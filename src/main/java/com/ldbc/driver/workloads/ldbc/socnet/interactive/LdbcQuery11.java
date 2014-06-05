package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.List;

public class LdbcQuery11 extends Operation<List<LdbcQuery11Result>> {
    public static final int DEFAULT_LIMIT = 10;

    private final long personId;
    private final String personUri;
    private final String countryName;
    private final int workFromYear;
    private final int limit;

    public LdbcQuery11(long personId, String personUri, String countryName, int workFromYear, int limit) {
        this.personId = personId;
        this.personUri = personUri;
        this.countryName = countryName;
        this.workFromYear = workFromYear;
        this.limit = limit;
    }

    public long personId() {
        return personId;
    }

    public String personUri() {
        return personUri;
    }

    public String countryName() {
        return countryName;
    }

    public int workFromYear() {
        return workFromYear;
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
        if (workFromYear != that.workFromYear) return false;
        if (countryName != null ? !countryName.equals(that.countryName) : that.countryName != null) return false;
        if (personUri != null ? !personUri.equals(that.personUri) : that.personUri != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (personUri != null ? personUri.hashCode() : 0);
        result = 31 * result + (countryName != null ? countryName.hashCode() : 0);
        result = 31 * result + workFromYear;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery11{" +
                "personId=" + personId +
                ", personUri='" + personUri + '\'' +
                ", countryName='" + countryName + '\'' +
                ", workFromYear=" + workFromYear +
                ", limit=" + limit +
                '}';
    }
}
