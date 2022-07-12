package org.ldbcouncil.snb.driver.workloads.interactive.queries;
/**
 * LdbcInsert2AddPostLike.java
 * 
 * Interactive workload insert query 2:
 * -- Add like to post --
 * 
 * Add like to post
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcOperation;

import java.util.Date;
import java.util.Map;

public class LdbcInsert2AddPostLike extends LdbcOperation<LdbcNoResult>
{
    public static final int TYPE = 1002;
    public static final String PERSON_ID = "personId";
    public static final String POST_ID = "postId";
    public static final String CREATION_DATE = "creationDate";

    private final long personId;
    private final long postId;
    private final Date creationDate;

    public LdbcInsert2AddPostLike(
        @JsonProperty("personId")     long personId,
        @JsonProperty("postId")       long postId,
        @JsonProperty("creationDate") Date creationDate
    )
    {
        this.personId = personId;
        this.postId = postId;
        this.creationDate = creationDate;
    }

    public long getPersonId()
    {
        return personId;
    }

    public long getPostId()
    {
        return postId;
    }

    public Date getCreationDate()
    {
        return creationDate;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personId)
                .put(POST_ID, postId)
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

        LdbcInsert2AddPostLike that = (LdbcInsert2AddPostLike) o;

        if ( personId != that.personId )
        { return false; }
        if ( postId != that.postId )
        { return false; }
        if ( creationDate != null ? !creationDate.equals( that.creationDate ) : that.creationDate != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (int) (postId ^ (postId >>> 32));
        result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcInsert2AddPostLike{" +
               "personId=" + personId +
               ", postId=" + postId +
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
