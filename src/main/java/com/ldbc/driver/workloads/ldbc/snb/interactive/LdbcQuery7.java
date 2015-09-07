package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class LdbcQuery7 extends Operation<List<LdbcQuery7Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 7;
    public static final int DEFAULT_LIMIT = 20;
    private final long personId;
    private final int limit;

    public LdbcQuery7( long personId, int limit )
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

        LdbcQuery7 that = (LdbcQuery7) o;

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
        return "LdbcQuery7{" +
               "personId=" + personId +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery7Result> marshalResult( String serializedResult ) throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList;
        try
        {
            resultsAsList = OBJECT_MAPPER.readValue( serializedResult, new TypeReference<List<List<Object>>>()
            {
            } );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException(
                    format( "Error parsing serialized result\n%s", serializedResult ), e );
        }

        List<LdbcQuery7Result> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long personId = ((Number) row.get( 0 )).longValue();
            String personFirstName = (String) row.get( 1 );
            String personLastName = (String) row.get( 2 );
            long likeCreationDate = ((Number) row.get( 3 )).longValue();
            long commentOrPostId = ((Number) row.get( 4 )).longValue();
            String commentOrPostContent = (String) row.get( 5 );
            int minutesLatency = ((Number) row.get( 6 )).intValue();
            boolean isNew = (Boolean) row.get( 7 );

            result.add( new LdbcQuery7Result(
                    personId,
                    personFirstName,
                    personLastName,
                    likeCreationDate,
                    commentOrPostId,
                    commentOrPostContent,
                    minutesLatency,
                    isNew
            ) );
        }

        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcQuery7Result> results = (List<LdbcQuery7Result>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < results.size(); i++ )
        {
            LdbcQuery7Result result = results.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( result.personId() );
            resultFields.add( result.personFirstName() );
            resultFields.add( result.personLastName() );
            resultFields.add( result.likeCreationDate() );
            resultFields.add( result.commentOrPostId() );
            resultFields.add( result.commentOrPostContent() );
            resultFields.add( result.minutesLatency() );
            resultFields.add( result.isNew() );
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
