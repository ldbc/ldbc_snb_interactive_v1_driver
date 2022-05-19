package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * LdbcQuery11.java
 * 
 * Interactive workload complex read query 11:
 * -- Job Referral --
 * 
 * Given a start Person, find that Person’s friends and friends of friends
 * (excluding start Person) who started working in some Company in a given
 * Country, before a given date (year).
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LdbcQuery11 extends Operation<List<LdbcQuery11Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 11;
    public static final int DEFAULT_LIMIT = 10;
    public static final String PERSON_ID = "personIdQ11";
    public static final String COUNTRY_NAME = "countryName";
    public static final String WORK_FROM_YEAR = "workFromYear";
    public static final String LIMIT = "limit";

    private final long personIdQ11;
    private final String countryName;
    private final int workFromYear;
    private final int limit;

    public LdbcQuery11(
        @JsonProperty("personIdQ11")     long personIdQ11,
        @JsonProperty("countryName")  String countryName,
        @JsonProperty("workFromYear") int workFromYear,
        @JsonProperty("limit")        int limit
    )
    {
        this.personIdQ11 = personIdQ11;
        this.countryName = countryName;
        this.workFromYear = workFromYear;
        this.limit = limit;
    }

    public long getPersonIdQ11()
    {
        return personIdQ11;
    }

    public String getCountryName()
    {
        return countryName;
    }

    public int getWorkFromYear()
    {
        return workFromYear;
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

        LdbcQuery11 that = (LdbcQuery11) o;

        if ( limit != that.limit )
        { return false; }
        if ( personIdQ11 != that.personIdQ11 )
        { return false; }
        if ( workFromYear != that.workFromYear )
        { return false; }
        if ( countryName != null ? !countryName.equals( that.countryName ) : that.countryName != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personIdQ11 ^ (personIdQ11 >>> 32));
        result = 31 * result + (countryName != null ? countryName.hashCode() : 0);
        result = 31 * result + workFromYear;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery11{" +
               "personIdQ11=" + personIdQ11 +
               ", countryName='" + countryName + '\'' +
               ", workFromYear=" + workFromYear +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery11Result> deserializeResult( String serializedResults ) throws IOException
    {
        List<LdbcQuery11Result> marshaledOperationResult;
        marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery11Result[].class));
        return marshaledOperationResult;
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
