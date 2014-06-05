package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.Date;
import java.util.List;

public class LdbcQuery3 extends Operation<List<LdbcQuery3Result>> {
    public static int DEFAULT_LIMIT = 20;
    private final long personId;
    private final String personUri;
    private final String countryX;
    private final String countryY;
    private final Date startDate;
    private final long durationMillis;
    private final int limit;

    // TODO duration days

    public LdbcQuery3(long personId, String personUri, String countryX, String countryY, Date startDate, long durationMillis, int limit) {
        super();
        this.personId = personId;
        this.personUri = personUri;
        this.countryX = countryX;
        this.countryY = countryY;
        this.startDate = startDate;
        this.durationMillis = durationMillis;
        this.limit = limit;
    }

    public long personId() {
        return personId;
    }

    public String personUri() {
        return personUri;
    }

    public String countryX() {
        return countryX;
    }

    public String countryY() {
        return countryY;
    }

    public Date startDate() {
        return startDate;
    }

    public long durationMillis() {
        return durationMillis;
    }

    public int limit() {
        return limit;
    }


}
