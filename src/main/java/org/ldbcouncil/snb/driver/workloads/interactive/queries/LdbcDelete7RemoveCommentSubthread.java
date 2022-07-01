package org.ldbcouncil.snb.driver.workloads.interactive.queries;

import java.util.Map;

import org.ldbcouncil.snb.driver.workloads.interactive.LdbcOperation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

public class LdbcDelete7RemoveCommentSubthread extends LdbcOperation<LdbcNoResult> {
    public static final int TYPE = 1015;
    public static final String COMMENT_ID = "commentId";

    private final long removeCommentIdD7;

    public LdbcDelete7RemoveCommentSubthread(
        @JsonProperty("removeCommentIdD7")    long removeCommentIdD7
    )
    {
        this.removeCommentIdD7 = removeCommentIdD7;
    }

    public long getremoveCommentIdD7()
    {
        return removeCommentIdD7;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(COMMENT_ID, removeCommentIdD7)
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

        if ( removeCommentIdD7 != that.removeCommentIdD7 )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (removeCommentIdD7 ^ (removeCommentIdD7 >>> 32));
    }

    @Override
    public String toString()
    {
        return "LdbcDelete7RemoveCommentSubthread{" +
               "removeCommentIdD7=" + removeCommentIdD7 +
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
