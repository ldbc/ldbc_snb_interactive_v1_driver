package org.ldbcouncil.snb.driver.workloads.interactive.queries;

import java.util.Map;

import org.ldbcouncil.snb.driver.Operation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;


public class LdbcDelete2RemovePostLike extends Operation<LdbcNoResult> {
    public static final int TYPE = 1010;
    public static final String PERSON_ID = "removePersonIdD2";
    public static final String POST_ID = "removePostIdD2";

    private final long removePostIdD2;
    private final long removePersonIdD2;

    public LdbcDelete2RemovePostLike(
        @JsonProperty("removePersonIdD2")    long removePersonIdD2,
        @JsonProperty("removePostIdD2")    long removePostIdD2
    )
    {
        this.removePostIdD2 = removePostIdD2;
        this.removePersonIdD2 = removePersonIdD2;
    }

    public long getremovePostIdD2()
    {
        return removePostIdD2;
    }

    public long getremovePersonIdD2()
    {
        return removePersonIdD2;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(POST_ID, removePostIdD2)
                .put(PERSON_ID, removePersonIdD2)
                .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcDelete2RemovePostLike that = (LdbcDelete2RemovePostLike) o;

        if ( removePostIdD2 != that.removePostIdD2 )
        { return false; }
        if ( removePersonIdD2 != that.removePersonIdD2 )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (removePostIdD2 ^ (removePostIdD2 >>> 32));
        result = 31 * result + (int) (removePersonIdD2 ^ (removePersonIdD2 >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcDelete2RemovePostLike{" +
               "removePostIdD2=" + removePostIdD2 +
               ", removePersonIdD2=" + removePersonIdD2 +
               '}';
    }


    @Override
    public LdbcNoResult deserializeResult( String serializedResults )
    {
        return LdbcNoResult.INSTANCE;
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
