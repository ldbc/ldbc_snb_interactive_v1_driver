package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * LdbcQuery5.java
 * 
 * Interactive workload complex read query 5:
 * -- New groups --
 * 
 * Given a start Person, denote their friends and friends of friends
 * (excluding the start Person) as otherPerson. Find Forums that any
 * Person otherPerson became a member of after a given date (minDate).
 * For each of those Forums, count the number of Posts that were
 * created by the Person otherPerson.
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

public class LdbcQuery5 extends Operation<List<LdbcQuery5Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 5;
    public static final int DEFAULT_LIMIT = 20;
    // Parameters used for replacement in queries
    public static final String PERSON_ID = "personId";
    public static final String MIN_DATE = "minDate";
    public static final String LIMIT = "limit";

    private final long personIdQ5;
    private final Date minDate;
    private final int limit;

    public LdbcQuery5(
        @JsonProperty("personIdQ5") long personIdQ5,
        @JsonProperty("minDate")    Date minDate,
        @JsonProperty("limit")      int limit
    )
    {
        super();
        this.personIdQ5 = personIdQ5;
        this.minDate = minDate;
        this.limit = limit;
    }

    public long getPersonIdQ5()
    {
        return personIdQ5;
    }

    public Date getMinDate()
    {
        return minDate;
    }

    public int getLimit()
    {
        return limit;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personIdQ5)
                .put(MIN_DATE, minDate)
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

        LdbcQuery5 that = (LdbcQuery5) o;

        if ( limit != that.limit )
        { return false; }
        if ( personIdQ5 != that.personIdQ5 )
        { return false; }
        if ( minDate != null ? !minDate.equals( that.minDate ) : that.minDate != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personIdQ5 ^ (personIdQ5 >>> 32));
        result = 31 * result + (minDate != null ? minDate.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery5{" +
               "personIdQ5=" + personIdQ5 +
               ", minDate=" + minDate +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery5Result> deserializeResult( String serializedResults ) throws IOException
    {
        List<LdbcQuery5Result> marshaledOperationResult;
        marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery5Result[].class));
        return marshaledOperationResult;
    }
  
    @Override
    public int type()
    {
        return TYPE;
    }
}
