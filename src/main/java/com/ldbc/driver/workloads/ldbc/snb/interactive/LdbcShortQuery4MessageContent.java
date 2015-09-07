package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class LdbcShortQuery4MessageContent extends Operation<LdbcShortQuery4MessageContentResult>
{
    public static final int TYPE = 104;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final long messageId;

    public LdbcShortQuery4MessageContent( long messageId )
    {
        this.messageId = messageId;
    }

    public long messageId()
    {
        return messageId;
    }

    @Override
    public LdbcShortQuery4MessageContentResult marshalResult( String serializedResult )
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

        String marshaledMessageContent = (String) resultAsList.get( 0 );
        long marshaledMessageCreationDate = ((Number) resultAsList.get( 1 )).longValue();

        return new LdbcShortQuery4MessageContentResult(
                marshaledMessageContent,
                marshaledMessageCreationDate
        );
    }

    @Override
    public String serializeResult( Object operationResultInstance ) throws SerializingMarshallingException
    {
        LdbcShortQuery4MessageContentResult result = (LdbcShortQuery4MessageContentResult) operationResultInstance;
        List<Object> resultFields = new ArrayList<>();
        resultFields.add( result.messageContent() );
        resultFields.add( result.messageCreationDate() );
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

        LdbcShortQuery4MessageContent that = (LdbcShortQuery4MessageContent) o;

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
        return "LdbcShortQuery4MessageContent{" +
               "messageId=" + messageId +
               '}';
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}