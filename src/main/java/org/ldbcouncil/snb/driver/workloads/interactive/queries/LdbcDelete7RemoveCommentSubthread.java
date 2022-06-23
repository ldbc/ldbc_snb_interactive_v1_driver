package org.ldbcouncil.snb.driver.workloads.interactive.queries;

import java.util.Map;

import org.ldbcouncil.snb.driver.Operation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;


public class LdbcDelete7RemoveCommentSubthread extends Operation<LdbcNoResult> {
    public static final int TYPE = 1015;
    public static final String COMMENT_ID = "commentId"; 

    private final long commentId;

    public LdbcDelete7RemoveCommentSubthread(
        @JsonProperty("commentId")    long commentId
    )
    {
        this.commentId = commentId;
    }

    public long getCommentId()
    {
        return commentId;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(COMMENT_ID, commentId)
                .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcDelete7RemoveCommentSubthread that = (LdbcDelete7RemoveCommentSubthread) o;

        if ( commentId != that.commentId )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (commentId ^ (commentId >>> 32));
    }

    @Override
    public String toString()
    {
        return "LdbcDelete7RemoveCommentSubthread{" +
               "commentId=" + commentId +
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
