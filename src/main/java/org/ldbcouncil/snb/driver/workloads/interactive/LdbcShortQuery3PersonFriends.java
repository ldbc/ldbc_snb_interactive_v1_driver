package org.ldbcouncil.snb.driver.workloads.interactive;

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
    public static final String PERSON_ID = "personIdQ3";

    private final long personIdQ3;

    public LdbcShortQuery3PersonFriends(@JsonProperty("personIdQ3")  long personIdQ3 )
    {
        this.personIdQ3 = personIdQ3;
    }

    public long getPersonIdQ3()
    {
        return personIdQ3;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personIdQ3)
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

        if ( personIdQ3 != that.personIdQ3 )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (personIdQ3 ^ (personIdQ3 >>> 32));
    }

    @Override
    public String toString()
    {
        return "LdbcShortQuery3PersonFriends{" +
               "personIdQ3=" + personIdQ3 +
               '}';
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}