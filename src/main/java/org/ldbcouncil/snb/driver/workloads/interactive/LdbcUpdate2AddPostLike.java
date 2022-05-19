package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * LdbcUpdate2AddPostLike.java
 * 
 * Interactive workload insert query 2:
 * -- Add like to post --
 * 
 * Add like to post
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.util.Date;
import java.util.Map;

public class LdbcUpdate2AddPostLike extends Operation<LdbcNoResult>
{
    public static final int TYPE = 1002;
    public static final String PERSON_ID = "personId";
    public static final String POST_ID = "postId";
    public static final String CREATION_DATE = "creationDate";

    private final long personId;
    private final long postId;
    private final Date creationDate;

    public LdbcUpdate2AddPostLike(
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
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcUpdate2AddPostLike that = (LdbcUpdate2AddPostLike) o;

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
        return "LdbcUpdate2AddPostLike{" +
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
