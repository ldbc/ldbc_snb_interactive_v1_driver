package org.ldbcouncil.snb.driver.workloads.interactive.queries;
/**
 * LdbcUpdate5AddForumMembership.java
 * 
 * Interactive workload insert query 5:
 * -- Add forum membership --
 * 
 * Add a Forum membership edge (hasMember) to a Person
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcOperation;

import java.util.Date;
import java.util.Map;

public class LdbcUpdate5AddForumMembership extends LdbcOperation<LdbcNoResult>
{
    public static final int TYPE = 1005;
    public static final String FORUM_ID = "forumId";
    public static final String PERSON_ID = "personId";
    public static final String CREATION_DATE = "creationDate";

    private final long forumId;
    private final long personId;
    private final Date creationDate;

    public LdbcUpdate5AddForumMembership(
        @JsonProperty("forumId")  long forumId,
        @JsonProperty("personId") long personId,
        @JsonProperty("creationDate") Date creationDate
    )
    {
        this.forumId = forumId;
        this.personId = personId;
        this.creationDate = creationDate;
    }

    public long getForumId()
    {
        return forumId;
    }

    public long getPersonId()
    {
        return personId;
    }

    public Date getCreationDate()
    {
        return creationDate;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(FORUM_ID, forumId)
                .put(PERSON_ID, personId)
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

        LdbcUpdate5AddForumMembership that = (LdbcUpdate5AddForumMembership) o;

        if ( forumId != that.forumId )
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
        int result = (int) (forumId ^ (forumId >>> 32));
        result = 31 * result + (int) (personId ^ (personId >>> 32));
        result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcUpdate5AddForumMembership{" +
               "forumId=" + forumId +
               ", personId=" + personId +
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
