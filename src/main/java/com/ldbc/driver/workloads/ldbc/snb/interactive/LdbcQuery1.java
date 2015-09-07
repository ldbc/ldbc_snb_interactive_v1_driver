package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class LdbcQuery1 extends Operation<List<LdbcQuery1Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 1;
    public static final int DEFAULT_LIMIT = 20;
    private final long personId;
    private final String firstName;
    private final int limit;

    public LdbcQuery1( long personId, String firstName, int limit )
    {
        this.personId = personId;
        this.firstName = firstName;
        this.limit = limit;
    }

    public long personId()
    {
        return personId;
    }

    public String firstName()
    {
        return firstName;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcQuery1 that = (LdbcQuery1) o;

        if ( limit != that.limit )
        { return false; }
        if ( personId != that.personId )
        { return false; }
        if ( firstName != null ? !firstName.equals( that.firstName ) : that.firstName != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (firstName != null ? firstName.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery1{" +
               "personId=" + personId +
               ", firstName='" + firstName + '\'' +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery1Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList;
        try
        {
            resultsAsList = OBJECT_MAPPER.readValue( serializedResults, new TypeReference<List<List<Object>>>()
            {
            } );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException(
                    format( "Error parsing serialized results\n%s", serializedResults ), e );
        }

        List<LdbcQuery1Result> results = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> resultAsList = resultsAsList.get( i );

            long friendId = ((Number) resultAsList.get( 0 )).longValue();
            String friendLastName = (String) resultAsList.get( 1 );
            int distanceFromPerson = (Integer) resultAsList.get( 2 );
            long friendBirthday = ((Number) resultAsList.get( 3 )).longValue();
            long friendCreationDate = ((Number) resultAsList.get( 4 )).longValue();
            String friendGender = (String) resultAsList.get( 5 );
            String friendBrowserUsed = (String) resultAsList.get( 6 );
            String friendLocationIp = (String) resultAsList.get( 7 );
            Iterable<String> friendEmails = (List<String>) resultAsList.get( 8 );
            Iterable<String> friendLanguages = (List<String>) resultAsList.get( 9 );
            String friendCityName = (String) resultAsList.get( 10 );
            Iterable<List<Object>> friendUniversities = Lists.newArrayList( (List) resultAsList.get( 11 ) );
            Iterable<List<Object>> friendCompanies = Lists.newArrayList( (List) resultAsList.get( 12 ) );

            results.add( new LdbcQuery1Result(
                    friendId,
                    friendLastName,
                    distanceFromPerson,
                    friendBirthday,
                    friendCreationDate,
                    friendGender,
                    friendBrowserUsed,
                    friendLocationIp,
                    friendEmails,
                    friendLanguages,
                    friendCityName,
                    friendUniversities,
                    friendCompanies ) );
        }

        return results;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcQuery1Result> results = (List<LdbcQuery1Result>) resultsObject;

        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < results.size(); i++ )
        {
            LdbcQuery1Result result = results.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( result.friendId() );
            resultFields.add( result.friendLastName() );
            resultFields.add( result.distanceFromPerson() );
            resultFields.add( result.friendBirthday() );
            resultFields.add( result.friendCreationDate() );
            resultFields.add( result.friendGender() );
            resultFields.add( result.friendBrowserUsed() );
            resultFields.add( result.friendLocationIp() );
            resultFields.add( result.friendEmails() );
            resultFields.add( result.friendLanguages() );
            resultFields.add( result.friendCityName() );
            resultFields.add( result.friendUniversities() );
            resultFields.add( result.friendCompanies() );
            resultsFields.add( resultFields );
        }

        try
        {
            return OBJECT_MAPPER.writeValueAsString( resultsFields );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException(
                    format( "Error while trying to serialize result\n%s", results.toString() ), e );
        }
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
