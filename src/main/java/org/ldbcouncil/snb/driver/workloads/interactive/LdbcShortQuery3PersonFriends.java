package org.ldbcouncil.snb.driver.workloads.interactive;

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
    public static final String PERSON_ID = "personId";

    private final long personId;

    public LdbcShortQuery3PersonFriends( long personId )
    {
        this.personId = personId;
    }

    public long getPersonId()
    {
        return personId;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personId)
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

        if ( personId != that.personId )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (personId ^ (personId >>> 32));
    }

    @Override
    public String toString()
    {
        return "LdbcShortQuery3PersonFriends{" +
               "personId=" + personId +
               '}';
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}