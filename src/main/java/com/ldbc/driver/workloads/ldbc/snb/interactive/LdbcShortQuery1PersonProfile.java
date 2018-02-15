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

public class LdbcShortQuery1PersonProfile extends Operation<LdbcShortQuery1PersonProfileResult>
{
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final int TYPE = 101;
    public static final String PERSON_ID = "personId";

    private final long personId;

    public LdbcShortQuery1PersonProfile( long personId )
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
    public LdbcShortQuery1PersonProfileResult marshalResult( String serializedResult )
            throws SerializingMarshallingException
    {
        List<Object> resultAsList;
        try
        {
            resultAsList = objectMapper.readValue( serializedResult, new TypeReference<List<Object>>()
            {
            } );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException( format( "Error while parsing serialized results\n%s",
                    serializedResult ), e );
        }

        String firstName = (String) resultAsList.get( 0 );
        String lastName = (String) resultAsList.get( 1 );
        long birthday = ((Number) resultAsList.get( 2 )).longValue();
        String locationIp = (String) resultAsList.get( 3 );
        String browserUsed = (String) resultAsList.get( 4 );
        long cityId = ((Number) resultAsList.get( 5 )).longValue();
        String gender = (String) resultAsList.get( 6 );
        long creationDate = ((Number) resultAsList.get( 7 )).longValue();

        return new LdbcShortQuery1PersonProfileResult(
                firstName,
                lastName,
                birthday,
                locationIp,
                browserUsed,
                cityId,
                gender,
                creationDate
        );
    }

    @Override
    public String serializeResult( Object operationResultInstance ) throws SerializingMarshallingException
    {
        LdbcShortQuery1PersonProfileResult result = (LdbcShortQuery1PersonProfileResult) operationResultInstance;
        List<Object> resultFields = new ArrayList<>();
        resultFields.add( result.firstName() );
        resultFields.add( result.lastName() );
        resultFields.add( result.birthday() );
        resultFields.add( result.locationIp() );
        resultFields.add( result.browserUsed() );
        resultFields.add( result.cityId() );
        resultFields.add( result.gender() );
        resultFields.add( result.creationDate() );
        try
        {
            return objectMapper.writeValueAsString( resultFields );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException( format( "Error while trying to serialize result\n%s",
                    result.toString() ), e );
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcShortQuery1PersonProfile that = (LdbcShortQuery1PersonProfile) o;

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
        return "LdbcShortQuery1PersonProfile{" +
               "personId=" + personId +
               '}';
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}