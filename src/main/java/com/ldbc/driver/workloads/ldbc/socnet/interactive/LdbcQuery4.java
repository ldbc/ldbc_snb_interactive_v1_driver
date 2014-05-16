package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class LdbcQuery4 extends Operation<List<LdbcQuery4Result>> {
    public static final int DEFAULT_LIMIT = 10;
    private final long personId;
    private final Date minDate;
    private final Date maxDate;
    private final long durationMillis;
    private final int limit;

    public LdbcQuery4(long personId, Date maxDate, long durationMillis, int limit) {
        super();
        this.personId = personId;
        this.maxDate = maxDate;
        this.durationMillis = durationMillis;
        this.limit = limit;
        Calendar c = Calendar.getInstance();
        c.clear();
        c.setTime(maxDate);
        this.minDate = new Date(c.getTimeInMillis() - durationMillis);

    }

    public long personId() {
        return personId;
    }

    public Date minDate() {
        return minDate;
    }

    public Date maxDate() {
        return maxDate;
    }

    public long durationMillis() {
        return durationMillis;
    }

    public long minDateAsMilli() {
        return minDate().getTime();
    }

    public long maxDateAsMilli() {
        return maxDate().getTime();
    }

    public int limit() {
        return limit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery4 that = (LdbcQuery4) o;

        if (durationMillis != that.durationMillis) return false;
        if (limit != that.limit) return false;
        if (personId != that.personId) return false;
        if (maxDate != null ? !maxDate.equals(that.maxDate) : that.maxDate != null) return false;
        if (minDate != null ? !minDate.equals(that.minDate) : that.minDate != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (minDate != null ? minDate.hashCode() : 0);
        result = 31 * result + (maxDate != null ? maxDate.hashCode() : 0);
        result = 31 * result + (int) (durationMillis ^ (durationMillis >>> 32));
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery4{" +
                "personId=" + personId +
                ", minDate=" + minDate +
                ", maxDate=" + maxDate +
                ", durationMillis=" + durationMillis +
                ", limit=" + limit +
                '}';
    }
}
