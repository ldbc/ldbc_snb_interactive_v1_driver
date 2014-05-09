package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class LdbcQuery4 extends Operation<List<LdbcQuery4Result>> {
    private final long personId;
    private final Date minDate;
    private final Date maxDate;
    private final long durationMillis;

    public LdbcQuery4(long personId, Date maxDate, long durationMillis) {
        super();
        this.personId = personId;
        this.maxDate = maxDate;
        this.durationMillis = durationMillis;
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (durationMillis ^ (durationMillis >>> 32));
        result = prime * result + ((maxDate == null) ? 0 : maxDate.hashCode());
        result = prime * result + (int) (personId ^ (personId >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        LdbcQuery4 other = (LdbcQuery4) obj;
        if (durationMillis != other.durationMillis) return false;
        if (maxDate == null) {
            if (other.maxDate != null) return false;
        } else if (!maxDate.equals(other.maxDate)) return false;
        if (personId != other.personId) return false;
        return true;
    }

    @Override
    public String toString() {
        return "LdbcQuery4 [personId=" + personId + ", minDate=" + minDate() + ", maxDate=" + maxDate()
                + ", minDateAsMilli=" + minDateAsMilli() + ", maxDateAsMilli=" + maxDateAsMilli() + ", durationMillis="
                + durationMillis + "]";
    }
}
