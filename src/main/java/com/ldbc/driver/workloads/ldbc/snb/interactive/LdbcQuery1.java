package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;

import java.util.List;

public class LdbcQuery1 extends Operation<List<LdbcQuery1Result>> {
    public static final int DEFAULT_LIMIT = 20;
    private final long personId;
    private final String personUri;
    private final String firstName;
    private final int limit;

    public LdbcQuery1(long personId, String personUri, String firstName, int limit) {
        this.personId = personId;
        this.personUri = personUri;
        this.firstName = firstName;
        this.limit = limit;
    }

    public long personId() {
        return personId;
    }

    public String personUri() {
        return personUri;
    }

    public String firstName() {
        return firstName;
    }

    public int limit() {
        return limit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery1 that = (LdbcQuery1) o;

        if (limit != that.limit) return false;
        if (personId != that.personId) return false;
        if (firstName != null ? !firstName.equals(that.firstName) : that.firstName != null) return false;
        if (personUri != null ? !personUri.equals(that.personUri) : that.personUri != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (personUri != null ? personUri.hashCode() : 0);
        result = 31 * result + (firstName != null ? firstName.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery1{" +
                "personId=" + personId +
                ", personUri='" + personUri + '\'' +
                ", firstName='" + firstName + '\'' +
                ", limit=" + limit +
                '}';
    }
}
