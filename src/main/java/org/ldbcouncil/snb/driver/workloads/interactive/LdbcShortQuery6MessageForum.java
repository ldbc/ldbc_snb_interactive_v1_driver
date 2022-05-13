package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Map;

public class LdbcShortQuery6MessageForum extends Operation<LdbcShortQuery6MessageForumResult>
{
    public static final int TYPE = 106;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String MESSAGE_ID = "messageForumId";

    private final long messageForumId;

    public LdbcShortQuery6MessageForum(@JsonProperty("messageForumId") long messageForumId )
    {
        this.messageForumId = messageForumId;
    }

    public long getMessageForumId()
    {
        return messageForumId;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(MESSAGE_ID, messageForumId)
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

        if ( messageForumId != that.messageForumId )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (messageForumId ^ (messageForumId >>> 32));
    }

    @Override
    public String toString()
    {
        return "LdbcShortQuery6MessageForum{" +
               "messageForumId=" + messageForumId +
               '}';
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
