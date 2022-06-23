package org.ldbcouncil.snb.driver.workloads.interactive.queries;

import java.util.Map;

import org.ldbcouncil.snb.driver.Operation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;


public class LdbcDelete6RemovePostThread extends Operation<LdbcNoResult> {
    public static final int TYPE = 1014;
    public static final String POST_ID = "postId"; 

    private final long postId;

    public LdbcDelete6RemovePostThread(
        @JsonProperty("postId")    long postId
    )
    {
        this.postId = postId;
    }

    public long getpostId()
    {
        return postId;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(POST_ID, postId)
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

        if ( postId != that.postId )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (postId ^ (postId >>> 32));
    }

    @Override
    public String toString()
    {
        return "LdbcDelete6RemovePostThread{" +
               "postId=" + postId +
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
