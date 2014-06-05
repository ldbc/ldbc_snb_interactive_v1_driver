package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.Date;
import java.util.List;

public class LdbcQuery5 extends Operation<List<LdbcQuery5Result>> {
    public static final int DEFAULT_LIMIT = 20;
    private final long personId;
    private final String personUri;
    private final Date minDate;
    private final int limit;

    public LdbcQuery5(long personId, String personUri, Date minDate, int limit) {
        super();
        this.personId = personId;
        this.personUri = personUri;
        this.minDate = minDate;
        this.limit = limit;
    }

    public long personId() {
        return personId;
    }

    public String personUri() {
        return personUri;
    }

    public Date minDate() {
        return minDate;
    }

    public int limit() {
        return limit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery5 that = (LdbcQuery5) o;

        if (limit != that.limit) return false;
        if (personId != that.personId) return false;
        if (minDate != null ? !minDate.equals(that.minDate) : that.minDate != null) return false;
        if (personUri != null ? !personUri.equals(that.personUri) : that.personUri != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (personUri != null ? personUri.hashCode() : 0);
        result = 31 * result + (minDate != null ? minDate.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery5{" +
                "personId=" + personId +
                ", personUri='" + personUri + '\'' +
                ", minDate=" + minDate +
                ", limit=" + limit +
                '}';
    }
}
