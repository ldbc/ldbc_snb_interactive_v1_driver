package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * LdbcShortQuery2PersonPosts.java
 * 
 * Interactive workload short read query 2:
 * -- Recent messages of a person --
 * 
 * Given a start Person, retrieve the last 10 Messages created by
 * that user. For each Message, return that Message, the original Post in 
 * its conversation (post), and the author of that Post (originalPoster).
 * If any of the Messages is a Post, then the original Post (post) will
 * be the same Message, i.e. that Message will appear twice in that result.
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LdbcShortQuery2PersonPosts extends Operation<List<LdbcShortQuery2PersonPostsResult>>
{
    public static final int TYPE = 102;
    public static final int DEFAULT_LIMIT = 10;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String PERSON_ID = "personIdSQ2";
    public static final String LIMIT = "limit";

    private final long personIdSQ2;
    private final int limit;

    public LdbcShortQuery2PersonPosts(
        @JsonProperty("personIdSQ2") long personIdSQ2,
        @JsonProperty("limit")       int limit
    )
    {
        this.personIdSQ2 = personIdSQ2;
        this.limit = limit;
    }

    public long getPersonIdSQ2()
    {
        return personIdSQ2;
    }

    public int getLimit()
    {
        return limit;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personIdSQ2)
                .put(LIMIT, limit)
                .build();
    }

    @Override
    public List<LdbcShortQuery2PersonPostsResult> deserializeResult( String serializedResults ) throws IOException
    {
        List<LdbcShortQuery2PersonPostsResult> marshaledOperationResult;
        marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcShortQuery2PersonPostsResult[].class));
        return marshaledOperationResult;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcShortQuery2PersonPosts that = (LdbcShortQuery2PersonPosts) o;

        if ( limit != that.limit )
        { return false; }
        if ( personIdSQ2 != that.personIdSQ2 )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personIdSQ2 ^ (personIdSQ2 >>> 32));
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcShortQuery2PersonPosts{" +
               "personIdSQ2=" + personIdSQ2 +
               ", limit=" + limit +
               '}';
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
