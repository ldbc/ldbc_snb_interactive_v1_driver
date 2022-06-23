package org.ldbcouncil.snb.driver.workloads.interactive.queries;

import java.util.Map;

import org.ldbcouncil.snb.driver.Operation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;


public class LdbcDelete3RemoveCommentLike extends Operation<LdbcNoResult> {
    public static final int TYPE = 1011;
    public static final String PERSON_ID = "personId";
    public static final String COMMENT_ID = "commentId";

    private final long commentId;
    private final long personId;

    public LdbcDelete3RemoveCommentLike(
        @JsonProperty("personId")    long personId,
        @JsonProperty("commentId")    long commentId
    )
    {
        this.commentId = commentId;
        this.personId = personId;
    }

    public long getcommentId()
    {
        return commentId;
    }

    public long getpersonId()
    {
        return personId;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(COMMENT_ID, commentId)
                .put(PERSON_ID, personId)
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

        if ( commentId != that.commentId )
        { return false; }
        if ( personId != that.personId )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (commentId ^ (commentId >>> 32));
        result = 31 * result + (int) (personId ^ (personId >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcDelete3RemoveCommentLike{" +
               "commentId=" + commentId +
               ", personId=" + personId +
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
