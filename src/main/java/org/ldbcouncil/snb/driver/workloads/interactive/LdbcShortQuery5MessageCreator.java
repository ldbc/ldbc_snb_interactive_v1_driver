package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Map;


public class LdbcShortQuery5MessageCreator extends Operation<LdbcShortQuery5MessageCreatorResult>
{
    public static final int TYPE = 105;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String MESSAGE_ID = "messageId";

    private final long messageId;

    public LdbcShortQuery5MessageCreator( long messageId )
    {
        this.messageId = messageId;
    }

    public long getMessageId()
    {
        return messageId;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(MESSAGE_ID, messageId)
                .build();
    }
    @Override
    public LdbcShortQuery5MessageCreatorResult deserializeResult( String serializedResults ) throws IOException
    {
        LdbcShortQuery5MessageCreatorResult marshaledOperationResult;
        marshaledOperationResult = OBJECT_MAPPER.readValue(serializedResults, LdbcShortQuery5MessageCreatorResult.class);
        return marshaledOperationResult;
    }
 
    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcShortQuery5MessageCreator that = (LdbcShortQuery5MessageCreator) o;

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
        return "LdbcShortQuery5MessageCreator{" +
               "messageId=" + messageId +
               '}';
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}