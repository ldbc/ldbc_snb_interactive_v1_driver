package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class LdbcShortQuery7MessageReplies extends Operation<List<LdbcShortQuery7MessageRepliesResult>>
{
    public static final int TYPE = 107;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final long messageId;

    public LdbcShortQuery7MessageReplies( long messageId )
    {
        this.messageId = messageId;
    }

    public long messageId()
    {
        return messageId;
    }

    @Override
    public List<LdbcShortQuery7MessageRepliesResult> marshalResult( String serializedResult )
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

        List<LdbcShortQuery7MessageRepliesResult> results = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> resultAsList = resultsAsList.get( i );

            long commentId = ((Number) resultAsList.get( 0 )).longValue();
            String commentContent = (String) resultAsList.get( 1 );
            long commentCreationDate = ((Number) resultAsList.get( 2 )).longValue();
            long replyAuthorId = ((Number) resultAsList.get( 3 )).longValue();
            String replyAuthorFirstName = (String) resultAsList.get( 4 );
            String replyAuthorLastName = (String) resultAsList.get( 5 );
            boolean isReplyAuthorKnowsOriginalMessageAuthor = (Boolean) resultAsList.get( 6 );

            results.add(
                    new LdbcShortQuery7MessageRepliesResult(
                            commentId,
                            commentContent,
                            commentCreationDate,
                            replyAuthorId,
                            replyAuthorFirstName,
                            replyAuthorLastName,
                            isReplyAuthorKnowsOriginalMessageAuthor
                    )
            );
        }
        return results;
    }

    @Override
    public String serializeResult( Object operationResultInstance ) throws SerializingMarshallingException
    {
        List<LdbcShortQuery7MessageRepliesResult> results =
                (List<LdbcShortQuery7MessageRepliesResult>) operationResultInstance;

        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < results.size(); i++ )
        {
            LdbcShortQuery7MessageRepliesResult result = results.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( result.commentId() );
            resultFields.add( result.commentContent() );
            resultFields.add( result.commentCreationDate() );
            resultFields.add( result.replyAuthorId() );
            resultFields.add( result.replyAuthorFirstName() );
            resultFields.add( result.replyAuthorLastName() );
            resultFields.add( result.isReplyAuthorKnowsOriginalMessageAuthor() );
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

        LdbcShortQuery7MessageReplies that = (LdbcShortQuery7MessageReplies) o;

        if ( messageId != that.messageId )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (messageId ^ (messageId >>> 32));
    }

    @Override
    public String toString()
    {
        return "LdbcShortQuery7MessageReplies{" +
               "messageId=" + messageId +
               '}';
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}