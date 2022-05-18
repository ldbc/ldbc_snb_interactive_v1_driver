package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LdbcShortQuery7MessageReplies extends Operation<List<LdbcShortQuery7MessageRepliesResult>>
{
    public static final int TYPE = 107;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String MESSAGE_ID = "messageRepliesId";

    private final long messageRepliesId;

    public LdbcShortQuery7MessageReplies(@JsonProperty("messageRepliesId")  long messageRepliesId )
    {
        this.messageRepliesId = messageRepliesId;
    }

    public long getMessageRepliesId()
    {
        return messageRepliesId;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(MESSAGE_ID, messageRepliesId)
                .build();
    }

    @Override
    public List<LdbcShortQuery7MessageRepliesResult> deserializeResult( String serializedResults ) throws IOException
    {
        List<LdbcShortQuery7MessageRepliesResult> marshaledOperationResult;
        marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcShortQuery7MessageRepliesResult[].class));
        return marshaledOperationResult;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcShortQuery7MessageReplies that = (LdbcShortQuery7MessageReplies) o;

        if ( messageRepliesId != that.messageRepliesId )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (messageRepliesId ^ (messageRepliesId >>> 32));
    }

    @Override
    public String toString()
    {
        return "LdbcShortQuery7MessageReplies{" +
               "messageRepliesId=" + messageRepliesId +
               '}';
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
