package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class LdbcShortQuery6MessageForum extends Operation<LdbcShortQuery6MessageForumResult>
{
    public static final int TYPE = 106;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final long messageId;

    public LdbcShortQuery6MessageForum( long messageId )
    {
        this.messageId = messageId;
    }

    public long messageId()
    {
        return messageId;
    }

    @Override
    public LdbcShortQuery6MessageForumResult marshalResult( String serializedResult )
            throws SerializingMarshallingException
    {
        List<Object> resultAsList;
        try
        {
            resultAsList = objectMapper.readValue( serializedResult, new TypeReference<List<Object>>()
            {
            } );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException( format( "Error while parsing serialized results\n%s",
                    serializedResult ), e );
        }

        long forumId = ((Number) resultAsList.get( 0 )).longValue();
        String forumTitle = (String) resultAsList.get( 1 );
        long moderatorId = ((Number) resultAsList.get( 2 )).longValue();
        String moderatorFirstName = (String) resultAsList.get( 3 );
        String moderatorLastName = (String) resultAsList.get( 4 );

        return new LdbcShortQuery6MessageForumResult(
                forumId,
                forumTitle,
                moderatorId,
                moderatorFirstName,
                moderatorLastName
        );
    }

    @Override
    public String serializeResult( Object operationResultInstance ) throws SerializingMarshallingException
    {
        LdbcShortQuery6MessageForumResult result = (LdbcShortQuery6MessageForumResult) operationResultInstance;
        List<Object> resultFields = new ArrayList<>();
        resultFields.add( result.forumId() );
        resultFields.add( result.forumTitle() );
        resultFields.add( result.moderatorId() );
        resultFields.add( result.moderatorFirstName() );
        resultFields.add( result.moderatorLastName() );

        try
        {
            return objectMapper.writeValueAsString( resultFields );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException( format( "Error while trying to serialize result\n%s",
                    result.toString() ), e );
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcShortQuery6MessageForum that = (LdbcShortQuery6MessageForum) o;

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
        return "LdbcShortQuery6MessageForum{" +
               "messageId=" + messageId +
               '}';
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}