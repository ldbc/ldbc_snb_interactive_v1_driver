package org.ldbcouncil.snb.driver.workloads.interactive.queries;
/**
 * LdbcQuery10.java
 * 
 * Interactive workload complex read query 10:
 * -- Friend recommendation --
 * 
 * Given a start Person with id personId, find that Person’s friends of friends (foaf) 
 * – excluding the start Person and his/her immediate friends –, who were born on or
 * after the 21st of a given month (in any year) and before the 22nd of the following
 * month. Calculate the similarity between each friend and the start person, where
 * commonInterestScore is defined as follows:
 * - common = number of Posts created by friend, such that the Post has a Tag that 
 *   the start person is interested in
 * - uncommon = number of Posts created by friend, such that the Post has no Tag
 *   that the start person is interested in
 * - commonInterestScore = common - uncommon
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LdbcQuery10 extends Operation<List<LdbcQuery10Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 10;
    public static final int DEFAULT_LIMIT = 10;
    // Parameters used for replacement in queries
    public static final String PERSON_ID = "personId";
    public static final String MONTH = "month";
    public static final String LIMIT = "limit";

    private final long personIdQ10;
    private final int month;
    private final int limit;

    public LdbcQuery10(
        @JsonProperty("personIdQ10") long personIdQ10,
        @JsonProperty("month")       int month,
        @JsonProperty("limit")       int limit
    )
    {
        this.personIdQ10 = personIdQ10;
        this.month = month;
        this.limit = limit;
    }

    public LdbcQuery10( LdbcQuery10 query )
    {
        this.personIdQ10 = query.getPersonIdQ10();
        this.month = query.getMonth();
        this.limit = query.getLimit();
    }

    public long getPersonIdQ10()
    {
        return personIdQ10;
    }

    public int getMonth()
    {
        return month;
    }

    public int getLimit()
    {
        return limit;
    }

    @Override
    public LdbcQuery10 newInstance(){
        return new LdbcQuery10(this);
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personIdQ10)
                .put(MONTH, month)
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

        LdbcQuery10 that = (LdbcQuery10) o;

        if ( limit != that.limit )
        { return false; }
        if ( month != that.month )
        { return false; }
        if ( personIdQ10 != that.personIdQ10 )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personIdQ10 ^ (personIdQ10 >>> 32));
        result = 31 * result + month;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery10{" +
               "personIdQ10=" + personIdQ10 +
               ", month=" + month +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery10Result> deserializeResult( String serializedResults ) throws IOException
    {
        List<LdbcQuery10Result> marshaledOperationResult;
        marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery10Result[].class));
        return marshaledOperationResult;
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
