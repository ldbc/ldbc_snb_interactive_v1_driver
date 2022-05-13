package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Map;

public class LdbcShortQuery6MessageForum extends Operation<LdbcShortQuery6MessageForumResult>
{
    public static final int TYPE = 106;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String MESSAGE_ID = "messageId";

    private final long messageId;

    public LdbcShortQuery6MessageForum( long messageId )
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
    public LdbcShortQuery6MessageForumResult deserializeResult( String serializedResults ) throws IOException
    {
        LdbcShortQuery6MessageForumResult marshaledOperationResult;
        marshaledOperationResult = OBJECT_MAPPER.readValue(serializedResults, LdbcShortQuery6MessageForumResult.class);
        return marshaledOperationResult;
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