package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * LdbcQuery8.java
 * 
 * Interactive workload complex read query 8:
 * -- Recent replies --
 * 
 * Given a start Person, find the most recent Comments that are replies 
 * to Messages of the start Person. Only consider direct (single-hop)
 * replies, not the transitive (multi-hop) ones. Return the reply Comments,
 * and the Person that created each reply Comment.
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LdbcQuery8 extends Operation<List<LdbcQuery8Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 8;
    public static final int DEFAULT_LIMIT = 20;
    public static final String PERSON_ID = "personIdQ8";
    public static final String LIMIT = "limit";

    private final long personIdQ8;
    private final int limit;

    public LdbcQuery8(
        @JsonProperty("personIdQ8") long personIdQ8,
        @JsonProperty("limit")      int limit
    )
    {
        super();
        this.personIdQ8 = personIdQ8;
        this.limit = limit;
    }

    public long getPersonIdQ8()
    {
        return personIdQ8;
    }

    public int getLimit()
    {
        return limit;
    }


    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personIdQ8)
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

        LdbcQuery8 that = (LdbcQuery8) o;

        if ( limit != that.limit )
        { return false; }
        if ( personIdQ8 != that.personIdQ8 )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personIdQ8 ^ (personIdQ8 >>> 32));
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery8{" +
               "personIdQ8=" + personIdQ8 +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery8Result> deserializeResult( String serializedResults ) throws IOException
    {
        List<LdbcQuery8Result> marshaledOperationResult;
        marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery8Result[].class));
        return marshaledOperationResult;
    }
  
    @Override
    public int type()
    {
        return TYPE;
    }
}
