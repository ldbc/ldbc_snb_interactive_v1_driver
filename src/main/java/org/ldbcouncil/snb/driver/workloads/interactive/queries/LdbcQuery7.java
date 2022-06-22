package org.ldbcouncil.snb.driver.workloads.interactive.queries;
/**
 * LdbcQuery7.java
 * 
 * Interactive workload complex read query 7:
 * -- Recent likers --
 * 
 * Given a start Person, find the most recent likes on any of start
 * Person’s Messages. Find Persons that liked (likes edge) any of
 * start Person’s Messages, the Messages they liked most recently,
 * the creation date of that like, and the latency in minutes
 * (minutesLatency) between creation of Messages and like.
 * Additionally, for each Person found return a flag indicating
 * (isNew) whether the liker is a friend of start Person. In case
 * that a Person liked multiple Messages at the same time, return the
 * Message with lowest identifier.
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LdbcQuery7 extends Operation<List<LdbcQuery7Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 7;
    public static final int DEFAULT_LIMIT = 20;
    // Parameters used for replacement in queries
    public static final String PERSON_ID = "personId";
    public static final String LIMIT = "limit";

    private final long personIdQ7;
    private final int limit;

    public LdbcQuery7(
        @JsonProperty("personIdQ7") long personIdQ7,
        @JsonProperty("limit")      int limit
    )
    {
        super();
        this.personIdQ7 = personIdQ7;
        this.limit = limit;
    }

    public LdbcQuery7( LdbcQuery7 query )
    {
        super();
        this.personIdQ7 = query.getPersonIdQ7();
        this.limit = query.getLimit();
    }

    public long getPersonIdQ7()
    {
        return personIdQ7;
    }

    public int getLimit()
    {
        return limit;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personIdQ7)
                .put(LIMIT, limit)
                .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcQuery7 that = (LdbcQuery7) o;

        if ( limit != that.limit )
        { return false; }
        if ( personIdQ7 != that.personIdQ7 )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personIdQ7 ^ (personIdQ7 >>> 32));
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery7{" +
               "personIdQ7=" + personIdQ7 +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery7Result> deserializeResult( String serializedResults ) throws IOException
    {
        List<LdbcQuery7Result> marshaledOperationResult;
        marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery7Result[].class));
        return marshaledOperationResult;
    }
   
    @Override
    public int type()
    {
        return TYPE;
    }
}
