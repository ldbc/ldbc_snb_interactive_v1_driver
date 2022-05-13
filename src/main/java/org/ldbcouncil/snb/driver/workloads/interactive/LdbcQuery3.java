package org.ldbcouncil.snb.driver.workloads.interactive;

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
    public static final String PERSON_ID = "personId";
    public static final String COUNTRY_X_NAME  = "countryXName";
    public static final String COUNTRY_Y_NAME  = "countryYName";
    public static final String START_DATE = "startDate";
    public static final String DURATION_DAYS= "durationDays";
    public static final String LIMIT = "limit";

    private final long personId;
    private final String countryXName;
    private final String countryYName;
    private final Date startDate;
    private final int durationDays;
    private final int limit;

    public LdbcQuery3( long personId, String countryXName, String countryYName, Date startDate, int durationDays,
            int limit )
    {
        this.personId = personId;
        this.countryXName = countryXName;
        this.countryYName = countryYName;
        this.startDate = startDate;
        this.durationDays = durationDays;
        this.limit = limit;
    }

    public long getPersonId()
    {
        return personId;
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
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
            .put(PERSON_ID, personId)
            .put(COUNTRY_X_NAME, countryXName)
            .put(COUNTRY_Y_NAME, countryYName)
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

        LdbcQuery3 that = (LdbcQuery3) o;

        if ( durationDays != that.durationDays )
        { return false; }
        if ( limit != that.limit )
        { return false; }
        if ( personId != that.personId )
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
        int result = (int) (personId ^ (personId >>> 32));
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
               "personId=" + personId +
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