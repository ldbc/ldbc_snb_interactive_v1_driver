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

public class LdbcQuery4 extends Operation<List<LdbcQuery4Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 4;
    public static final int DEFAULT_LIMIT = 10;
    private final long personId;
    private final Date startDate;
    private final int durationDays;
    private final int limit;

    public LdbcQuery4( long personId, Date startDate, int durationDays, int limit )
    {
        this.personId = personId;
        this.startDate = startDate;
        this.durationDays = durationDays;
        this.limit = limit;
    }

    public long personId()
    {
        return personId;
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

        LdbcQuery4 that = (LdbcQuery4) o;

        if ( durationDays != that.durationDays )
        { return false; }
        if ( limit != that.limit )
        { return false; }
        if ( personId != that.personId )
        { return false; }
        if ( startDate != null ? !startDate.equals( that.startDate ) : that.startDate != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (startDate != null ? startDate.hashCode() : 0);
        result = 31 * result + durationDays;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery4{" +
               "personId=" + personId +
               ", startDate=" + startDate +
               ", durationDays=" + durationDays +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery4Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
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

        List<LdbcQuery4Result> results = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> resultAsList = resultsAsList.get( i );
            String tagName = (String) resultAsList.get( 0 );
            int tagCount = ((Number) resultAsList.get( 1 )).intValue();

            results.add( new LdbcQuery4Result(
                    tagName,
                    tagCount
            ) );
        }

        return results;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcQuery4Result> results = (List<LdbcQuery4Result>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < results.size(); i++ )
        {
            LdbcQuery4Result result = results.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( result.tagName() );
            resultFields.add( result.postCount() );
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