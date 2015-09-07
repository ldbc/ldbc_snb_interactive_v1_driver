package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class LdbcQuery6 extends Operation<List<LdbcQuery6Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 6;
    public static final int DEFAULT_LIMIT = 10;
    private final long personId;
    private final String tagName;
    private final int limit;

    public LdbcQuery6( long personId, String tagName, int limit )
    {
        this.personId = personId;
        this.tagName = tagName;
        this.limit = limit;
    }

    public long personId()
    {
        return personId;
    }

    public String tagName()
    {
        return tagName;
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

        LdbcQuery6 that = (LdbcQuery6) o;

        if ( limit != that.limit )
        { return false; }
        if ( personId != that.personId )
        { return false; }
        if ( tagName != null ? !tagName.equals( that.tagName ) : that.tagName != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (tagName != null ? tagName.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery6{" +
               "personId=" + personId +
               ", tagName='" + tagName + '\'' +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery6Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
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

        List<LdbcQuery6Result> results = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> resultAsList = resultsAsList.get( i );
            String tagName = (String) resultAsList.get( 0 );
            int tagCount = ((Number) resultAsList.get( 1 )).intValue();

            results.add( new LdbcQuery6Result(
                    tagName,
                    tagCount
            ) );
        }

        return results;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcQuery6Result> results = (List<LdbcQuery6Result>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < results.size(); i++ )
        {
            LdbcQuery6Result result = results.get( i );
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
