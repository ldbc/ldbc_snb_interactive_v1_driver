package org.ldbcouncil.snb.driver.workloads.interactive.queries;

import java.util.Map;

import org.ldbcouncil.snb.driver.Operation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;


public class LdbcDelete4RemoveForum extends Operation<LdbcNoResult> {
    public static final int TYPE = 1012;
    public static final String FORUM_ID = "removeForumIdD4";

    private final long removeForumIdD4;

    public LdbcDelete4RemoveForum(
        @JsonProperty("removeForumIdD4")    long removeForumIdD4
    )
    {
        this.removeForumIdD4 = removeForumIdD4;
    }

    public long getremoveForumIdD4()
    {
        return removeForumIdD4;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(FORUM_ID, removeForumIdD4)
                .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcDelete4RemoveForum that = (LdbcDelete4RemoveForum) o;

        if ( removeForumIdD4 != that.removeForumIdD4 )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (removeForumIdD4 ^ (removeForumIdD4 >>> 32));
    }

    @Override
    public String toString()
    {
        return "LdbcDelete4RemoveForum{" +
               "removeForumIdD4=" + removeForumIdD4 +
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
