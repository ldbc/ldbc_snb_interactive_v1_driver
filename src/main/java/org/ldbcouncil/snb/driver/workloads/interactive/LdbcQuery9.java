package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * LdbcQuery9.java
 * 
 * Interactive workload complex read query 9:
 * -- Recent messages by friends or friends of friends --
 * 
 * Given a start Person, find the most recent Messages created by
 * that Person’s friends or friends of friends (excluding start Person).
 * Only consider Messages created before the given maxDate (excluding that day).
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class LdbcQuery9 extends Operation<List<LdbcQuery9Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 9;
    public static final int DEFAULT_LIMIT = 20;
    // Parameters used for replacement in queries
    public static final String PERSON_ID = "personId";
    public static final String MAX_DATE = "maxDate";
    public static final String LIMIT = "limit";

    private final long personIdQ9;
    private final Date maxDate;
    private final int limit;

    public LdbcQuery9(
        @JsonProperty("personIdQ9") long personIdQ9,
        @JsonProperty("maxDate")    Date maxDate,
        @JsonProperty("limit")      int limit
    )
    {
        this.personIdQ9 = personIdQ9;
        this.maxDate = maxDate;
        this.limit = limit;
    }

    public long getPersonIdQ9()
    {
        return personIdQ9;
    }

    public Date getMaxDate()
    {
        return maxDate;
    }

    public int getLimit()
    {
        return limit;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personIdQ9)
                .put(MAX_DATE, maxDate)
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

        LdbcQuery9 that = (LdbcQuery9) o;

        if ( limit != that.limit )
        { return false; }
        if ( personIdQ9 != that.personIdQ9 )
        { return false; }
        if ( maxDate != null ? !maxDate.equals( that.maxDate ) : that.maxDate != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personIdQ9 ^ (personIdQ9 >>> 32));
        result = 31 * result + (maxDate != null ? maxDate.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery9{" +
               "personIdQ9=" + personIdQ9 +
               ", maxDate=" + maxDate +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery9Result> deserializeResult( String serializedResults ) throws IOException
    {
        List<LdbcQuery9Result> marshaledOperationResult;
        marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery9Result[].class));
        return marshaledOperationResult;
    }
  
    @Override
    public int type()
    {
        return TYPE;
    }
}
