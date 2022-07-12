package org.ldbcouncil.snb.driver.workloads.interactive.queries;
/**
 * LdbcInsert3AddCommentLike.java
 * 
 * Interactive workload insert query 3:
 * -- Add like to comment --
 * 
 * Add a likes edge to a Comment.
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcOperation;

import java.util.Date;
import java.util.Map;

public class LdbcInsert3AddCommentLike extends LdbcOperation<LdbcNoResult>
{
    public static final int TYPE = 1003;
    public static final String PERSON_ID = "personId";
    public static final String COMMENT_ID = "commentId";
    public static final String CREATION_DATE = "creationDate";

    private final long personId;
    private final long commentId;
    private final Date creationDate;

    public LdbcInsert3AddCommentLike(
        @JsonProperty("personId")     long personId,
        @JsonProperty("commentId")    long commentId,
        @JsonProperty("creationDate") Date creationDate
    )
    {
        this.personId = personId;
        this.commentId = commentId;
        this.creationDate = creationDate;
    }

    public long getPersonId()
    {
        return personId;
    }

    public long getCommentId()
    {
        return commentId;
    }

    public Date getCreationDate()
    {
        return creationDate;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personId)
                .put(COMMENT_ID, commentId)
                .put(CREATION_DATE, creationDate)
                .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcInsert3AddCommentLike that = (LdbcInsert3AddCommentLike) o;

        if ( commentId != that.commentId )
        { return false; }
        if ( personId != that.personId )
        { return false; }
        if ( creationDate != null ? !creationDate.equals( that.creationDate ) : that.creationDate != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (int) (commentId ^ (commentId >>> 32));
        result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcInsert3AddCommentLike{" +
               "personId=" + personId +
               ", commentId=" + commentId +
               ", creationDate=" + creationDate +
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
