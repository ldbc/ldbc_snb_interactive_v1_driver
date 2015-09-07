package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class LdbcQuery12 extends Operation<List<LdbcQuery12Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 12;
    public static final int DEFAULT_LIMIT = 20;
    private final long personId;
    private final String tagClassName;
    private final int limit;

    public LdbcQuery12( long personId, String tagClassName, int limit )
    {
        this.personId = personId;
        this.tagClassName = tagClassName;
        this.limit = limit;
    }

    public long personId()
    {
        return personId;
    }

    public String tagClassName()
    {
        return tagClassName;
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

        LdbcQuery12 that = (LdbcQuery12) o;

        if ( limit != that.limit )
        { return false; }
        if ( personId != that.personId )
        { return false; }
        if ( tagClassName != null ? !tagClassName.equals( that.tagClassName ) : that.tagClassName != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (tagClassName != null ? tagClassName.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery12{" +
               "personId=" + personId +
               ", tagClassName='" + tagClassName + '\'' +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery12Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
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

        List<LdbcQuery12Result> results = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> resultAsList = resultsAsList.get( i );
            long personId = ((Number) resultAsList.get( 0 )).longValue();
            String personFirstName = (String) resultAsList.get( 1 );
            String personLastName = (String) resultAsList.get( 2 );
            Iterable<String> tagNames = (List<String>) resultAsList.get( 3 );
            int replyCount = ((Number) resultAsList.get( 4 )).intValue();

            results.add( new LdbcQuery12Result(
                    personId,
                    personFirstName,
                    personLastName,
                    tagNames,
                    replyCount
            ) );
        }

        return results;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcQuery12Result> results = (List<LdbcQuery12Result>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < results.size(); i++ )
        {
            LdbcQuery12Result result = results.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( result.personId() );
            resultFields.add( result.personFirstName() );
            resultFields.add( result.personLastName() );
            resultFields.add( result.tagNames() );
            resultFields.add( result.replyCount() );
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
