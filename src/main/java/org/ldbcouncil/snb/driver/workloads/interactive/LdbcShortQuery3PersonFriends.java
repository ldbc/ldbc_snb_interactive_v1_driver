package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * LdbcShortQuery3PersonFriends.java
 * 
 * Interactive workload short read query 3:
 * -- Friends of a person --
 * 
 * Given a start Person, retrieve all of their friends,
 * and the date at which they became friends.
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LdbcShortQuery3PersonFriends extends Operation<List<LdbcShortQuery3PersonFriendsResult>>
{
    public static final int TYPE = 103;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String PERSON_ID = "personIdSQ3";

    private final long personIdSQ3;

    public LdbcShortQuery3PersonFriends(
        @JsonProperty("personIdSQ3") long personIdSQ3
    )
    {
        this.personIdSQ3 = personIdSQ3;
    }

    public long getPersonIdSQ3()
    {
        return personIdSQ3;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personIdSQ3)
                .build();
    }

    @Override
    public List<LdbcShortQuery3PersonFriendsResult> deserializeResult( String serializedResults ) throws IOException
    {
        List<LdbcShortQuery3PersonFriendsResult> marshaledOperationResult;
        marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcShortQuery3PersonFriendsResult[].class));
        return marshaledOperationResult;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcShortQuery3PersonFriends that = (LdbcShortQuery3PersonFriends) o;

        if ( personIdSQ3 != that.personIdSQ3 )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (personIdSQ3 ^ (personIdSQ3 >>> 32));
    }

    @Override
    public String toString()
    {
        return "LdbcShortQuery3PersonFriends{" +
               "personIdSQ3=" + personIdSQ3 +
               '}';
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
