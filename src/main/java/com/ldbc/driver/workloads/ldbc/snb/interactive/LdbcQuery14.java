package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.lang.String.format;

public class LdbcQuery14 extends Operation<List<LdbcQuery14Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 14;
    private final long personId;
    private final Date maxDate;

    public LdbcQuery14( long personId, Date maxDate )
    {
        this.personId = personId;
        this.maxDate = maxDate;
    }

    public long personId()
    {
        return personId;
    }

    public Date maxDate()
    {
        return maxDate;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcQuery14 that = (LdbcQuery14) o;

        if ( personId != that.personId )
        { return false; }
	if ( maxDate != null ? !maxDate.equals( that.maxDate ) : that.maxDate != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
	result = 31 * result + (maxDate != null ? maxDate.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery14{" +
               "personId=" + personId +
               ", maxDate=" + maxDate +
               '}';
    }

    @Override
    public List<LdbcQuery14Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList;
        try
        {
            resultsAsList = OBJECT_MAPPER.readValue(
                    serializedResults,
                    new TypeReference<List<List<Object>>>()
                    {
                    }
            );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException(
                    format( "Error while parsing serialized results\n%s", serializedResults ), e );
        }

        List<LdbcQuery14Result> results = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> resultAsList = resultsAsList.get( i );
	    String link = (String) resultAsList.get( 0 );
            int linkCount = ((Number) resultAsList.get( 1 )).intValue();

            results.add( new LdbcQuery14Result(
                    link,
                    linkCount
            ) );
        }
        return results;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcQuery14Result> results = (List<LdbcQuery14Result>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < results.size(); i++ )
        {
            LdbcQuery14Result result = results.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( result.link() );
            resultFields.add( result.linkCount() );
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
