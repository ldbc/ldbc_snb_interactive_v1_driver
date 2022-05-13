package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Map;

public class LdbcShortQuery4MessageContent extends Operation<LdbcShortQuery4MessageContentResult>
{
    public static final int TYPE = 104;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String MESSAGE_ID = "messageId";

    private final long messageId;

    public LdbcShortQuery4MessageContent( long messageId )
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
    public LdbcShortQuery4MessageContentResult deserializeResult( String serializedResults ) throws IOException
    {
        LdbcShortQuery4MessageContentResult marshaledOperationResult;
        marshaledOperationResult = OBJECT_MAPPER.readValue(serializedResults, LdbcShortQuery4MessageContentResult.class);
        return marshaledOperationResult;
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