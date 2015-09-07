package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class LdbcQuery11 extends Operation<List<LdbcQuery11Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 11;
    public static final int DEFAULT_LIMIT = 10;
    private final long personId;
    private final String countryName;
    private final int workFromYear;
    private final int limit;

    public LdbcQuery11( long personId, String countryName, int workFromYear, int limit )
    {
        this.personId = personId;
        this.countryName = countryName;
        this.workFromYear = workFromYear;
        this.limit = limit;
    }

    public long personId()
    {
        return personId;
    }

    public String countryName()
    {
        return countryName;
    }

    public int workFromYear()
    {
        return workFromYear;
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

        LdbcQuery11 that = (LdbcQuery11) o;

        if ( limit != that.limit )
        { return false; }
        if ( personId != that.personId )
        { return false; }
        if ( workFromYear != that.workFromYear )
        { return false; }
        if ( countryName != null ? !countryName.equals( that.countryName ) : that.countryName != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (countryName != null ? countryName.hashCode() : 0);
        result = 31 * result + workFromYear;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery11{" +
               "personId=" + personId +
               ", countryName='" + countryName + '\'' +
               ", workFromYear=" + workFromYear +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery11Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
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
                    format( "Error while parsing serialized results\n%s", serializedResults ), e );
        }

        List<LdbcQuery11Result> results = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> resultAsList = resultsAsList.get( i );
            long personId = ((Number) resultAsList.get( 0 )).longValue();
            String personFirstName = (String) resultAsList.get( 1 );
            String personLastName = (String) resultAsList.get( 2 );
            String organizationName = (String) resultAsList.get( 3 );
            int organizationWorkFromYear = ((Number) resultAsList.get( 4 )).intValue();

            results.add( new LdbcQuery11Result(
                    personId,
                    personFirstName,
                    personLastName,
                    organizationName,
                    organizationWorkFromYear
            ) );
        }

        return results;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcQuery11Result> results = (List<LdbcQuery11Result>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < results.size(); i++ )
        {
            LdbcQuery11Result result = results.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( result.personId() );
            resultFields.add( result.personFirstName() );
            resultFields.add( result.personLastName() );
            resultFields.add( result.organizationName() );
            resultFields.add( result.organizationWorkFromYear() );
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
