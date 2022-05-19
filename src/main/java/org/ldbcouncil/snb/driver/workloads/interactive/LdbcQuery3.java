package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * LdbcQuery3.java
 * 
 * Interactive workload complex read query 3:
 * -- Friends and friends of friends that have been to given countries --
 * 
 * Given a start Person, find Persons that are their friends and friends of friends
 * (excluding start Person) that have made Posts / Comments in both of the given
 * Countries, CountryX and CountryY, within a given period. Only Persons that are 
 * foreign to Countries CountryX and CountryY are considered, that is Persons whose
 * location is neither CountryX nor CountryY.
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

public class LdbcQuery3 extends Operation<List<LdbcQuery3Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 3;
    public static final int DEFAULT_LIMIT = 20;
    public static final String PERSON_ID = "personIdQ3";
    public static final String COUNTRY_X_NAME  = "countryXName";
    public static final String COUNTRY_Y_NAME  = "countryYName";
    public static final String START_DATE = "startDate";
    public static final String DURATION_DAYS= "durationDays";
    public static final String LIMIT = "limit";

    private final long personIdQ3;
    private final String countryXName;
    private final String countryYName;
    private final Date startDate;
    private final int durationDays;
    private final int limit;

    public LdbcQuery3(
        @JsonProperty("personIdQ3")   long personIdQ3,
        @JsonProperty("countryXName") String countryXName,
        @JsonProperty("countryYName") String countryYName,
        @JsonProperty("startDate")    Date startDate,
        @JsonProperty("durationDays") int durationDays,
        @JsonProperty("limit")        int limit
    )
    {
        this.personIdQ3 = personIdQ3;
        this.countryXName = countryXName;
        this.countryYName = countryYName;
        this.startDate = startDate;
        this.durationDays = durationDays;
        this.limit = limit;
    }

    public long getPersonIdQ3()
    {
        return personIdQ3;
    }

    public String getCountryXName()
    {
        return countryXName;
    }

    public String getCountryYName()
    {
        return countryYName;
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
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcQuery3 that = (LdbcQuery3) o;

        if ( durationDays != that.durationDays )
        { return false; }
        if ( limit != that.limit )
        { return false; }
        if ( personIdQ3 != that.personIdQ3 )
        { return false; }
        if ( countryXName != null ? !countryXName.equals( that.countryXName ) : that.countryXName != null )
        { return false; }
        if ( countryYName != null ? !countryYName.equals( that.countryYName ) : that.countryYName != null )
        { return false; }
        if ( startDate != null ? !startDate.equals( that.startDate ) : that.startDate != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personIdQ3 ^ (personIdQ3 >>> 32));
        result = 31 * result + (countryXName != null ? countryXName.hashCode() : 0);
        result = 31 * result + (countryYName != null ? countryYName.hashCode() : 0);
        result = 31 * result + (startDate != null ? startDate.hashCode() : 0);
        result = 31 * result + durationDays;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery3{" +
               "personIdQ3=" + personIdQ3 +
               ", countryXName='" + countryXName + '\'' +
               ", countryYName='" + countryYName + '\'' +
               ", startDate=" + startDate +
               ", durationDays=" + durationDays +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery3Result> deserializeResult( String serializedResults ) throws IOException
    {
        List<LdbcQuery3Result> marshaledOperationResult;
        marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery3Result[].class));
        return marshaledOperationResult;
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
