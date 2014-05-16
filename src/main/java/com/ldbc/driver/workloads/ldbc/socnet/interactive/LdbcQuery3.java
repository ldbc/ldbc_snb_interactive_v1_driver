package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class LdbcQuery3 extends Operation<List<LdbcQuery3Result>> {
    public static int DEFAULT_LIMIT = 20;
    private final long personId;
    private final String countryX;
    private final String countryY;
    private final Date endDate;
    private final long durationMillis;
    private final int limit;

    public LdbcQuery3(long personId, String countryX, String countryY, Date endDate, long durationMillis, int limit) {
        super();
        this.personId = personId;
        this.countryX = countryX;
        this.countryY = countryY;
        this.endDate = endDate;
        this.durationMillis = durationMillis;
        this.limit = limit;
    }

    public long personId() {
        return personId;
    }

    public String countryX() {
        return countryX;
    }

    public String countryY() {
        return countryY;
    }

    public Date endDate() {
        return endDate;
    }

    public long durationMillis() {
        return durationMillis;
    }

    public long startDateAsMilli() {
        Calendar c = Calendar.getInstance();
        c.clear();
        c.setTime(endDate);
        return c.getTimeInMillis() - durationMillis;
    }

    public long endDateAsMilli() {
        return endDate.getTime();
    }

    public int limit() {
        return limit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery3 that = (LdbcQuery3) o;

        if (durationMillis != that.durationMillis) return false;
        if (limit != that.limit) return false;
        if (personId != that.personId) return false;
        if (countryX != null ? !countryX.equals(that.countryX) : that.countryX != null) return false;
        if (countryY != null ? !countryY.equals(that.countryY) : that.countryY != null) return false;
        if (endDate != null ? !endDate.equals(that.endDate) : that.endDate != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (countryX != null ? countryX.hashCode() : 0);
        result = 31 * result + (countryY != null ? countryY.hashCode() : 0);
        result = 31 * result + (endDate != null ? endDate.hashCode() : 0);
        result = 31 * result + (int) (durationMillis ^ (durationMillis >>> 32));
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery3{" +
                "personId=" + personId +
                ", countryX='" + countryX + '\'' +
                ", countryY='" + countryY + '\'' +
                ", endDate=" + endDate +
                ", durationMillis=" + durationMillis +
                ", limit=" + limit +
                '}';
    }
}
