package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;

import java.util.Date;
import java.util.List;

public class LdbcQuery4 extends Operation<List<LdbcQuery4Result>> {
    public static final int DEFAULT_LIMIT = 10;
    private final long personId;
    private final String personUri;
    private final Date startDate;
    private final int durationDays;
    private final int limit;

    public LdbcQuery4(long personId, String personUri, Date startDate, int durationDays, int limit) {
        this.personId = personId;
        this.personUri = personUri;
        this.startDate = startDate;
        this.durationDays = durationDays;
        this.limit = limit;
    }

    public long personId() {
        return personId;
    }

    public String personUri() {
        return personUri;
    }

    public Date startDate() {
        return startDate;
    }

    public int durationDays() {
        return durationDays;
    }

    public int limit() {
        return limit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery4 that = (LdbcQuery4) o;

        if (durationDays != that.durationDays) return false;
        if (limit != that.limit) return false;
        if (personId != that.personId) return false;
        if (personUri != null ? !personUri.equals(that.personUri) : that.personUri != null) return false;
        if (startDate != null ? !startDate.equals(that.startDate) : that.startDate != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (personUri != null ? personUri.hashCode() : 0);
        result = 31 * result + (startDate != null ? startDate.hashCode() : 0);
        result = 31 * result + durationDays;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery4{" +
                "personId=" + personId +
                ", personUri='" + personUri + '\'' +
                ", startDate=" + startDate +
                ", durationDays=" + durationDays +
                ", limit=" + limit +
                '}';
    }
}