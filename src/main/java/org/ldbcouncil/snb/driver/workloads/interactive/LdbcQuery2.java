package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * LdbcQuery2.java
 * 
 * Interactive workload complex read query 2:
 * -- Recent messages by your friends --
 * 
 * Given a start Person (person), find the most recent Messages from all of that
 * Person’s friends (friend nodes). Only consider Messages created before the 
 * given maxDate (excluding that day).
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

public class LdbcQuery2 extends Operation<List<LdbcQuery2Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 2;
    public static final int DEFAULT_LIMIT = 20;
    public static final String PERSON_ID = "personIdQ2";
    public static final String MAX_DATE = "maxDate";
    public static final String LIMIT = "limit";

    private final long personIdQ2;
    private final Date maxDate;
    private final int limit;

    public LdbcQuery2(
        @JsonProperty("personIdQ2") long personIdQ2,
        @JsonProperty("maxDate")    Date maxDate,
        @JsonProperty("limit")      int limit
    )
    {
        super();
        this.personIdQ2 = personIdQ2;
        this.maxDate = maxDate;
        this.limit = limit;
    }

    public long getPersonIdQ2()
    {
        return personIdQ2;
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
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcQuery2 that = (LdbcQuery2) o;

        if ( limit != that.limit )
        { return false; }
        if ( personIdQ2 != that.personIdQ2 )
        { return false; }
        if ( maxDate != null ? !maxDate.equals( that.maxDate ) : that.maxDate != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personIdQ2 ^ (personIdQ2 >>> 32));
        result = 31 * result + (maxDate != null ? maxDate.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery2{" +
               "personIdQ2=" + personIdQ2 +
               ", maxDate=" + maxDate +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery2Result> deserializeResult( String serializedResults ) throws IOException
    {
        List<LdbcQuery2Result> marshaledOperationResult;
        marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery2Result[].class));
        return marshaledOperationResult;
    }
  
    @Override
    public int type()
    {
        return TYPE;
    }
}
