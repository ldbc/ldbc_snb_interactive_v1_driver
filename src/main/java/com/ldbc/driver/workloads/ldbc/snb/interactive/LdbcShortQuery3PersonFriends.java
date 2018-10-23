package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public class LdbcShortQuery3PersonFriends extends Operation<List<LdbcShortQuery3PersonFriendsResult>>
{
    public static final int TYPE = 103;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final String PERSON_ID = "personId";

    private final long personId;

    public LdbcShortQuery3PersonFriends( long personId )
    {
        this.personId = personId;
    }

    public long personId()
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
    public List<LdbcShortQuery3PersonFriendsResult> marshalResult( String serializedResult )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList;
        try
        {
            resultsAsList = objectMapper.readValue( serializedResult, new TypeReference<List<List<Object>>>()
            {
            } );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException( format( "Error while parsing serialized results\n%s",
                    serializedResult ), e );
        }

        List<LdbcShortQuery3PersonFriendsResult> results = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> resultAsList = resultsAsList.get( i );

            long friendId = ((Number) resultAsList.get( 0 )).longValue();
            String firstName = (String) resultAsList.get( 1 );
            String lastName = (String) resultAsList.get( 2 );
            long friendshipCreationDate = ((Number) resultAsList.get( 3 )).longValue();

            results.add(
                    new LdbcShortQuery3PersonFriendsResult(
                            friendId,
                            firstName,
                            lastName,
                            friendshipCreationDate
                    )
            );
        }
        return results;
    }

    @Override
    public String serializeResult( Object operationResultInstance ) throws SerializingMarshallingException
    {
        List<LdbcShortQuery3PersonFriendsResult> results =
                (List<LdbcShortQuery3PersonFriendsResult>) operationResultInstance;

        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < results.size(); i++ )
        {
            LdbcShortQuery3PersonFriendsResult result = results.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( result.personId() );
            resultFields.add( result.firstName() );
            resultFields.add( result.lastName() );
            resultFields.add( result.friendshipCreationDate() );
            resultsFields.add( resultFields );
        }

        try
        {
            return objectMapper.writeValueAsString( resultsFields );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException( format( "Error while trying to serialize result\n%s",
                    results.toString() ), e );
        }
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