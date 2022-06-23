package org.ldbcouncil.snb.driver.workloads.interactive.queries;

import java.util.Map;

import org.ldbcouncil.snb.driver.Operation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;


public class LdbcDelete5RemoveForumMembership extends Operation<LdbcNoResult> {
    public static final int TYPE = 1013;
    public static final String FORUM_ID = "forumId"; 
    public static final String PERSON_ID = "personId"; 

    private final long forumId;
    private final long personId;

    public LdbcDelete5RemoveForumMembership(
        @JsonProperty("forumId")    long forumId,
        @JsonProperty("personId")    long personId
    )
    {
        this.forumId = forumId;
        this.personId = personId;
    }

    public long getforumId()
    {
        return forumId;
    }

    public long getpersonId()
    {
        return personId;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(FORUM_ID, forumId)
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

        LdbcDelete5RemoveForumMembership that = (LdbcDelete5RemoveForumMembership) o;

        if ( forumId != that.forumId )
        { return false; }
        if ( personId != that.personId )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (forumId ^ (forumId >>> 32));
        result = 31 * result + (int) (personId ^ (personId >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcDelete5RemoveForumMembership{" +
               "forumId=" + forumId +
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
