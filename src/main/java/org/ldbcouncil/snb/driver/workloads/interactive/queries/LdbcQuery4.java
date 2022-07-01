package org.ldbcouncil.snb.driver.workloads.interactive.queries;
/**
 * LdbcQuery4.java
 * 
 * Interactive workload complex read query 4:
 * -- New topics --
 * 
 * Given a start Person (personId), find Tags that are attached to Posts
 * that were created by that Person’s friends. Only include Tags that 
 * were attached to friends’ Posts created within a given time interval,
 * and that were never attached to friends’ Posts created before this
 * interval.
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcOperation;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class LdbcQuery4 extends LdbcOperation<List<LdbcQuery4Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 4;
    public static final int DEFAULT_LIMIT = 10;
    // Parameters used for replacement in queries
    public static final String PERSON_ID = "personId";
    public static final String START_DATE = "startDate";
    public static final String DURATION_DAYS = "durationDays";
    public static final String LIMIT = "limit";

    private final long personIdQ4;
    private final Date startDate;
    private final int durationDays;
    private final int limit;

    public LdbcQuery4(
        @JsonProperty("personIdQ4")   long personIdQ4,
        @JsonProperty("startDate")    Date startDate,
        @JsonProperty("durationDays") int durationDays,
        @JsonProperty("limit")        int limit
    )
    {
        this.personIdQ4 = personIdQ4;
        this.startDate = startDate;
        this.durationDays = durationDays;
        this.limit = limit;
    }

    public LdbcQuery4( LdbcQuery4 query )
    {
        this.personIdQ4 = query.getPersonIdQ4();
        this.startDate = query.getStartDate();
        this.durationDays = query.getDurationDays();
        this.limit = query.getLimit();
    }

    public long getPersonIdQ4()
    {
        return personIdQ4;
    }

    public Date getStartDate()
    {
        return startDate;
    }

    public int getDurationDays()
    {
        return durationDays;
    }

    public int getLimit()
    {
        return limit;
    }

    @Override
    public LdbcQuery4 newInstance(){
        return new LdbcQuery4(this);
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personIdQ4)
                .put(START_DATE, startDate)
                .put(DURATION_DAYS, durationDays)
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

        LdbcQuery4 that = (LdbcQuery4) o;

        if ( durationDays != that.durationDays )
        { return false; }
        if ( limit != that.limit )
        { return false; }
        if ( personIdQ4 != that.personIdQ4 )
        { return false; }
        if ( startDate != null ? !startDate.equals( that.startDate ) : that.startDate != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personIdQ4 ^ (personIdQ4 >>> 32));
        result = 31 * result + (startDate != null ? startDate.hashCode() : 0);
        result = 31 * result + durationDays;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery4{" +
               "personIdQ4=" + personIdQ4 +
               ", startDate=" + startDate +
               ", durationDays=" + durationDays +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery4Result> deserializeResult( String serializedResults ) throws IOException
    {
        List<LdbcQuery4Result> marshaledOperationResult;
        marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery4Result[].class));
        return marshaledOperationResult;
    }
  
    @Override
    public int type()
    {
        return TYPE;
    }
}
