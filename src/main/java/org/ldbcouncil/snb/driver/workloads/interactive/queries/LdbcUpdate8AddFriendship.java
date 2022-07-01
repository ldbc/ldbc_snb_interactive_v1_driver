package org.ldbcouncil.snb.driver.workloads.interactive.queries;
/**
 * LdbcUpdate8AddFriendship.java
 * 
 * Interactive workload insert query 8:
 * -- Add friendship --
 * 
 * Add a friendship edge (knows) between two Persons.
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcOperation;

import java.util.Date;
import java.util.Map;

public class LdbcUpdate8AddFriendship extends LdbcOperation<LdbcNoResult>
{
    public static final int TYPE = 1008;
    public static final String PERSON1_ID = "person1Id";
    public static final String PERSON2_ID = "person2Id";
    public static final String CREATION_DATE = "creationDate";

    private final long person1Id;
    private final long person2Id;
    private final Date creationDate;

    public LdbcUpdate8AddFriendship(
        @JsonProperty("person1Id")    long person1Id,
        @JsonProperty("person2Id")    long person2Id,
        @JsonProperty("creationDate") Date creationDate
    )
    {
        this.person1Id = person1Id;
        this.person2Id = person2Id;
        this.creationDate = creationDate;
    }

    public long getPerson1Id()
    {
        return person1Id;
    }

    public long getPerson2Id()
    {
        return person2Id;
    }

    public Date getCreationDate()
    {
        return creationDate;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON1_ID, person1Id)
                .put(PERSON2_ID, person2Id)
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

        LdbcUpdate8AddFriendship that = (LdbcUpdate8AddFriendship) o;

        if ( person1Id != that.person1Id )
        { return false; }
        if ( person2Id != that.person2Id )
        { return false; }
        if ( creationDate != null ? !creationDate.equals( that.creationDate ) : that.creationDate != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (person1Id ^ (person1Id >>> 32));
        result = 31 * result + (int) (person2Id ^ (person2Id >>> 32));
        result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcUpdate8AddFriendship{" +
               "person1Id=" + person1Id +
               ", person2Id=" + person2Id +
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
