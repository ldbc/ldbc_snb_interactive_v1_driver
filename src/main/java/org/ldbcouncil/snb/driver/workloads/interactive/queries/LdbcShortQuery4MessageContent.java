package org.ldbcouncil.snb.driver.workloads.interactive.queries;
/**
 * LdbcShortQuery4MessageContent.java
 * 
 * Interactive workload short read query 4:
 * -- Content of a message --
 * 
 * Given a Message, retrieve its content and creation date.
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Map;

public class LdbcShortQuery4MessageContent extends Operation<LdbcShortQuery4MessageContentResult>
{
    public static final int TYPE = 104;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    // Parameters used for replacement in queries
    public static final String MESSAGE_ID = "messageId";

    private final long messageIdContent;

    public LdbcShortQuery4MessageContent(
        @JsonProperty("messageIdContent") long messageIdContent
    )
    {
        this.messageIdContent = messageIdContent;
    }

    public long getMessageIdContent()
    {
        return messageIdContent;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(MESSAGE_ID, messageIdContent)
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
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcShortQuery4MessageContent that = (LdbcShortQuery4MessageContent) o;

        if ( messageIdContent != that.messageIdContent )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (messageIdContent ^ (messageIdContent >>> 32));
    }

    @Override
    public String toString()
    {
        return "LdbcShortQuery4MessageContent{" +
               "messageIdContent=" + messageIdContent +
               '}';
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
