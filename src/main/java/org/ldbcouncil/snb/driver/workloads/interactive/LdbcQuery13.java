package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Map;

public class LdbcQuery13 extends Operation<LdbcQuery13Result>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 13;
    public static final String PERSON1_ID = "person1Id";
    public static final String PERSON2_ID = "person2Id";

    private final long person1Id;
    private final long person2Id;

    public LdbcQuery13(
        @JsonProperty("person1Id") long person1Id,
        @JsonProperty("person2Id") long person2Id
    )
    {
        this.person1Id = person1Id;
        this.person2Id = person2Id;
    }

    public long getPerson1Id()
    {
        return person1Id;
    }

    public long getPerson2Id()
    {
        return person2Id;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON1_ID, person1Id)
                .put(PERSON2_ID, person2Id)
                .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcQuery13 that = (LdbcQuery13) o;

        if ( person1Id != that.person1Id )
        { return false; }
        if ( person2Id != that.person2Id )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (person1Id ^ (person1Id >>> 32));
        result = 31 * result + (int) (person2Id ^ (person2Id >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery13{" +
               "person1Id=" + person1Id +
               ", person2Id=" + person2Id +
               '}';
    }

    @Override
    public LdbcQuery13Result deserializeResult( String serializedResults ) throws IOException
    {
        LdbcQuery13Result marshaledOperationResult;
        marshaledOperationResult = OBJECT_MAPPER.readValue(serializedResults, LdbcQuery13Result.class);
        return marshaledOperationResult;
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
