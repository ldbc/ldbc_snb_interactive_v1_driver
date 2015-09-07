package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class LdbcShortQuery2PersonPosts extends Operation<List<LdbcShortQuery2PersonPostsResult>>
{
    public static final int TYPE = 102;
    public static final int DEFAULT_LIMIT = 10;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final long personId;
    private final int limit;

    public LdbcShortQuery2PersonPosts( long personId, int limit )
    {
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
    public List<LdbcShortQuery2PersonPostsResult> marshalResult( String serializedResult )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList;
        try
        {
            resultsAsList = objectMapper.readValue( serializedResult, new TypeReference<List<List<Object>>>()
            {
            } );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException( format( "Error while parsing serialized results\n%s",
                    serializedResult ), e );
        }

        List<LdbcShortQuery2PersonPostsResult> results = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> resultAsList = resultsAsList.get( i );

            long messageId = ((Number) resultAsList.get( 0 )).longValue();
            String messageContent = (String) resultAsList.get( 1 );
            long messageCreationDate = ((Number) resultAsList.get( 2 )).longValue();
            long originalPostId = ((Number) resultAsList.get( 3 )).longValue();
            long originalPostAuthorId = ((Number) resultAsList.get( 4 )).longValue();
            String originalPostAuthorFirstName = (String) resultAsList.get( 5 );
            String originalPostAuthorLastName = (String) resultAsList.get( 6 );

            results.add(
                    new LdbcShortQuery2PersonPostsResult(
                            messageId,
                            messageContent,
                            messageCreationDate,
                            originalPostId,
                            originalPostAuthorId,
                            originalPostAuthorFirstName,
                            originalPostAuthorLastName
                    )
            );
        }
        return results;
    }

    @Override
    public String serializeResult( Object operationResultInstance ) throws SerializingMarshallingException
    {
        List<LdbcShortQuery2PersonPostsResult> results =
                (List<LdbcShortQuery2PersonPostsResult>) operationResultInstance;

        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < results.size(); i++ )
        {
            LdbcShortQuery2PersonPostsResult result = results.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( result.messageId() );
            resultFields.add( result.messageContent() );
            resultFields.add( result.messageCreationDate() );
            resultFields.add( result.originalPostId() );
            resultFields.add( result.originalPostAuthorId() );
            resultFields.add( result.originalPostAuthorFirstName() );
            resultFields.add( result.originalPostAuthorLastName() );
            resultsFields.add( resultFields );
        }

        try
        {
            return objectMapper.writeValueAsString( resultsFields );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException( format( "Error while trying to serialize result\n%s",
                    results.toString() ), e );
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcShortQuery2PersonPosts that = (LdbcShortQuery2PersonPosts) o;

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
        return "LdbcShortQuery2PersonPosts{" +
               "personId=" + personId +
               ", limit=" + limit +
               '}';
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}