package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * LdbcShortQuery5MessageCreator.java
 * 
 * Interactive workload short read query 5:
 * -- Creator of a message --
 * 
 * Given a Message, retrieve its author.
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Map;


public class LdbcShortQuery5MessageCreator extends Operation<LdbcShortQuery5MessageCreatorResult>
{
    public static final int TYPE = 105;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String MESSAGE_ID = "messageIdCreator";

    private final long messageIdCreator;

    public LdbcShortQuery5MessageCreator(
        @JsonProperty("messageIdCreator") long messageIdCreator
    )
    {
        this.messageIdCreator = messageIdCreator;
    }

    public long getMessageIdCreator()
    {
        return messageIdCreator;
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

        if ( messageIdCreator != that.messageIdCreator )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (messageIdCreator ^ (messageIdCreator >>> 32));
    }

    @Override
    public String toString()
    {
        return "LdbcShortQuery5MessageCreator{" +
               "messageIdCreator=" + messageIdCreator +
               '}';
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
