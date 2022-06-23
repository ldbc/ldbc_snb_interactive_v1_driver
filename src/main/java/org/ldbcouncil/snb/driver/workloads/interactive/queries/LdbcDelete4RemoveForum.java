package org.ldbcouncil.snb.driver.workloads.interactive.queries;

import java.util.Map;

import org.ldbcouncil.snb.driver.Operation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;


public class LdbcDelete4RemoveForum extends Operation<LdbcNoResult> {
    public static final int TYPE = 1012;
    public static final String FORUM_ID = "forumId";

    private final long forumId;

    public LdbcDelete4RemoveForum(
        @JsonProperty("forumId")    long forumId
    )
    {
        this.forumId = forumId;
    }

    public long getforumId()
    {
        return forumId;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(FORUM_ID, forumId)
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

        if ( forumId != that.forumId )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (forumId ^ (forumId >>> 32));
    }

    @Override
    public String toString()
    {
        return "LdbcDelete4RemoveForum{" +
               "forumId=" + forumId +
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
