package org.ldbcouncil.snb.driver.workloads.interactive.queries;

import java.util.Map;

import org.ldbcouncil.snb.driver.Operation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;


public class LdbcDelete3RemoveCommentLike extends Operation<LdbcNoResult> {
    public static final int TYPE = 1011;
    public static final String PERSON_ID = "removePersonIdD3";
    public static final String COMMENT_ID = "removeCommentIdD3";

    private final long removeCommentIdD3;
    private final long removePersonIdD3;

    public LdbcDelete3RemoveCommentLike(
        @JsonProperty("removePersonIdD3")    long removePersonIdD3,
        @JsonProperty("removeCommentIdD3")    long removeCommentIdD3
    )
    {
        this.removeCommentIdD3 = removeCommentIdD3;
        this.removePersonIdD3 = removePersonIdD3;
    }

    public long getremoveCommentIdD3()
    {
        return removeCommentIdD3;
    }

    public long getremovePersonIdD3()
    {
        return removePersonIdD3;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(COMMENT_ID, removeCommentIdD3)
                .put(PERSON_ID, removePersonIdD3)
                .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcDelete3RemoveCommentLike that = (LdbcDelete3RemoveCommentLike) o;

        if ( removeCommentIdD3 != that.removeCommentIdD3 )
        { return false; }
        if ( removePersonIdD3 != that.removePersonIdD3 )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (removeCommentIdD3 ^ (removeCommentIdD3 >>> 32));
        result = 31 * result + (int) (removePersonIdD3 ^ (removePersonIdD3 >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcDelete3RemoveCommentLike{" +
               "removeCommentIdD3=" + removeCommentIdD3 +
               ", removePersonIdD3=" + removePersonIdD3 +
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
