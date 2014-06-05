package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.List;

public class LdbcQuery7 extends Operation<List<LdbcQuery7Result>> {
    public static final int DEFAULT_LIMIT = 20;

    private final long personId;
    private final String personUri;
    private final int limit;

    public LdbcQuery7(long personId, String personUri, int limit) {
        super();
        this.personId = personId;
        this.personUri = personUri;
        this.limit = limit;
    }

    public long personId() {
        return personId;
    }

    public String personUri() {
        return personUri;
    }

    public int limit() {
        return limit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery7 that = (LdbcQuery7) o;

        if (limit != that.limit) return false;
        if (personId != that.personId) return false;
        if (personUri != null ? !personUri.equals(that.personUri) : that.personUri != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (personUri != null ? personUri.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery7{" +
                "personId=" + personId +
                ", personUri='" + personUri + '\'' +
                ", limit=" + limit +
                '}';
    }
}
