package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.Date;
import java.util.List;

public class LdbcQuery4 extends Operation<List<LdbcQuery4Result>> {
    public static final int DEFAULT_LIMIT = 10;
    private final long personId;
    private final String personUri;
    private final Date minDate;
    private final long durationMillis;
    private final int limit;

    // TODO duration days

    public LdbcQuery4(long personId, String personUri, Date minDate, long durationMillis, int limit) {
        super();
        this.personId = personId;
        this.personUri = personUri;
        this.minDate = minDate;
        this.durationMillis = durationMillis;
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

    public long durationMillis() {
        return durationMillis;
    }

    public int limit() {
        return limit;
    }
}
