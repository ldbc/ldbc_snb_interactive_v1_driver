package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.lang.String.format;

public class LdbcQuery3 extends Operation<List<LdbcQuery3Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 3;
    public static final int DEFAULT_LIMIT = 20;
    private final long personId;
    private final String countryXName;
    private final String countryYName;
    private final Date startDate;
    private final int durationDays;
    private final int limit;

    public LdbcQuery3( long personId, String countryXName, String countryYName, Date startDate, int durationDays,
            int limit )
    {
        this.personId = personId;
        this.countryXName = countryXName;
        this.countryYName = countryYName;
        this.startDate = startDate;
        this.durationDays = durationDays;
        this.limit = limit;
    }

    public long personId()
    {
        return personId;
    }

    public String countryXName()
    {
        return countryXName;
    }

    public String countryYName()
    {
        return countryYName;
    }

    public Date startDate()
    {
        return startDate;
    }

    public int durationDays()
    {
        return durationDays;
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

        LdbcQuery3 that = (LdbcQuery3) o;

        if ( durationDays != that.durationDays )
        { return false; }
        if ( limit != that.limit )
        { return false; }
        if ( personId != that.personId )
        { return false; }
        if ( countryXName != null ? !countryXName.equals( that.countryXName ) : that.countryXName != null )
        { return false; }
        if ( countryYName != null ? !countryYName.equals( that.countryYName ) : that.countryYName != null )
        { return false; }
        if ( startDate != null ? !startDate.equals( that.startDate ) : that.startDate != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (countryXName != null ? countryXName.hashCode() : 0);
        result = 31 * result + (countryYName != null ? countryYName.hashCode() : 0);
        result = 31 * result + (startDate != null ? startDate.hashCode() : 0);
        result = 31 * result + durationDays;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery3{" +
               "personId=" + personId +
               ", countryXName='" + countryXName + '\'' +
               ", countryYName='" + countryYName + '\'' +
               ", startDate=" + startDate +
               ", durationDays=" + durationDays +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery3Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
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

        List<LdbcQuery3Result> results = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> resultAsList = resultsAsList.get( i );
            long personId = ((Number) resultAsList.get( 0 )).longValue();
            String personFirstName = (String) resultAsList.get( 1 );
            String personLastName = (String) resultAsList.get( 2 );
            long xCount = ((Number) resultAsList.get( 3 )).longValue();
            long yCount = ((Number) resultAsList.get( 4 )).longValue();
            long count = ((Number) resultAsList.get( 5 )).longValue();

            results.add( new LdbcQuery3Result(
                    personId,
                    personFirstName,
                    personLastName,
                    xCount,
                    yCount,
                    count
            ) );
        }

        return results;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcQuery3Result> results = (List<LdbcQuery3Result>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < results.size(); i++ )
        {
            LdbcQuery3Result result = results.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( result.personId() );
            resultFields.add( result.personFirstName() );
            resultFields.add( result.personLastName() );
            resultFields.add( result.xCount() );
            resultFields.add( result.yCount() );
            resultFields.add( result.count() );
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