package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;

import java.util.Date;
import java.util.List;

public class LdbcQuery3 extends Operation<List<LdbcQuery3Result>> {
    public static int DEFAULT_LIMIT = 20;
    private final long personId;
    private final String personUri;
    private final String countryXName;
    private final String countryYName;
    private final Date startDate;
    private final int durationDays;
    private final int limit;

    public LdbcQuery3(long personId, String personUri, String countryXName, String countryYName, Date startDate, int durationDays, int limit) {
        this.personId = personId;
        this.personUri = personUri;
        this.countryXName = countryXName;
        this.countryYName = countryYName;
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

    public String countryXName() {
        return countryXName;
    }

    public String countryYName() {
        return countryYName;
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

        LdbcQuery3 that = (LdbcQuery3) o;

        if (durationDays != that.durationDays) return false;
        if (limit != that.limit) return false;
        if (personId != that.personId) return false;
        if (countryXName != null ? !countryXName.equals(that.countryXName) : that.countryXName != null) return false;
        if (countryYName != null ? !countryYName.equals(that.countryYName) : that.countryYName != null) return false;
        if (personUri != null ? !personUri.equals(that.personUri) : that.personUri != null) return false;
        if (startDate != null ? !startDate.equals(that.startDate) : that.startDate != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (personUri != null ? personUri.hashCode() : 0);
        result = 31 * result + (countryXName != null ? countryXName.hashCode() : 0);
        result = 31 * result + (countryYName != null ? countryYName.hashCode() : 0);
        result = 31 * result + (startDate != null ? startDate.hashCode() : 0);
        result = 31 * result + durationDays;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery3{" +
                "personId=" + personId +
                ", personUri='" + personUri + '\'' +
                ", countryXName='" + countryXName + '\'' +
                ", countryYName='" + countryYName + '\'' +
                ", startDate=" + startDate +
                ", durationDays=" + durationDays +
                ", limit=" + limit +
                '}';
    }
}