package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = LdbcNoResultJsonSerializer.class)
public class LdbcNoResult {
    public static final LdbcNoResult INSTANCE = new LdbcNoResult();

    private LdbcNoResult() {
    }
}
