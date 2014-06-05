package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.List;

public class LdbcQuery10 extends Operation<List<LdbcQuery10Result>> {
    public static final int DEFAULT_LIMIT = 20;

    private final long personId;
    private final String personUri;
    private final int month1;
    private final int month2;
    private final int limit;

    public LdbcQuery10(long personId, String personUri, int month1, int month2, int limit) {
        this.personId = personId;
        this.personUri = personUri;
        this.month1 = month1;
        this.month2 = month2;
        this.limit = limit;
    }

    public long personId() {
        return personId;
    }

    public String personUri() {
        return personUri;
    }

    public int month1() {
        return month1;
    }

    public int month2() {
        return month2;
    }

    public int limit() {
        return limit;
    }


}
