package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class LdbcQuery8 extends Operation<List<LdbcQuery8Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 8;
    public static final int DEFAULT_LIMIT = 20;
    private final long personId;
    private final int limit;

    public LdbcQuery8( long personId, int limit )
    {
        super();
        this.personId = personId;
        this.limit = limit;
    }

    public long personId()
    {
        return personId;
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

        LdbcQuery8 that = (LdbcQuery8) o;

        if ( limit != that.limit )
        { return false; }
        if ( personId != that.personId )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery8{" +
               "personId=" + personId +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery8Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
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

        List<LdbcQuery8Result> results = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> resultAsList = resultsAsList.get( i );
            long personId = ((Number) resultAsList.get( 0 )).longValue();
            String personFirstName = (String) resultAsList.get( 1 );
            String personLastName = (String) resultAsList.get( 2 );
            long commentCreationDate = ((Number) resultAsList.get( 3 )).longValue();
            long commentId = ((Number) resultAsList.get( 4 )).longValue();
            String commentContent = (String) resultAsList.get( 5 );

            results.add( new LdbcQuery8Result(
                    personId,
                    personFirstName,
                    personLastName,
                    commentCreationDate,
                    commentId,
                    commentContent
            ) );
        }

        return results;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcQuery8Result> results = (List<LdbcQuery8Result>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < results.size(); i++ )
        {
            LdbcQuery8Result result = results.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( result.personId() );
            resultFields.add( result.personFirstName() );
            resultFields.add( result.personLastName() );
            resultFields.add( result.commentCreationDate() );
            resultFields.add( result.commentId() );
            resultFields.add( result.commentContent() );
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
