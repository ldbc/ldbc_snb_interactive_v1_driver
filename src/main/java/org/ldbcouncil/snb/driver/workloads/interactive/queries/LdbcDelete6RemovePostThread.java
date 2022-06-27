package org.ldbcouncil.snb.driver.workloads.interactive.queries;

import java.util.Map;

import org.ldbcouncil.snb.driver.Operation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;


public class LdbcDelete6RemovePostThread extends Operation<LdbcNoResult> {
    public static final int TYPE = 1014;
    public static final String POST_ID = "postId";

    private final long removePostIdD6;

    public LdbcDelete6RemovePostThread(
        @JsonProperty("removePostIdD6")    long removePostIdD6
    )
    {
        this.removePostIdD6 = removePostIdD6;
    }

    public long getremovePostIdD6()
    {
        return removePostIdD6;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(POST_ID, removePostIdD6)
                .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcDelete6RemovePostThread that = (LdbcDelete6RemovePostThread) o;

        if ( removePostIdD6 != that.removePostIdD6 )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (removePostIdD6 ^ (removePostIdD6 >>> 32));
    }

    @Override
    public String toString()
    {
        return "LdbcDelete6RemovePostThread{" +
               "removePostIdD6=" + removePostIdD6 +
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
