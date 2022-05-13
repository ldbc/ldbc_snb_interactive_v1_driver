package org.ldbcouncil.snb.driver.workloads.interactive;

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
    public static final String PERSON_ID = "personId";
    public static final String COUNTRY_NAME = "countryName";
    public static final String WORK_FROM_YEAR = "workFromYear";
    public static final String LIMIT = "limit";

    private final long personId;
    private final String countryName;
    private final int workFromYear;
    private final int limit;

    public LdbcQuery11( long personId, String countryName, int workFromYear, int limit )
    {
        this.personId = personId;
        this.countryName = countryName;
        this.workFromYear = workFromYear;
        this.limit = limit;
    }

    public long getPersonId()
    {
        return personId;
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
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personId)
                .put(COUNTRY_NAME, countryName)
                .put(WORK_FROM_YEAR, workFromYear)
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

        LdbcQuery11 that = (LdbcQuery11) o;

        if ( limit != that.limit )
        { return false; }
        if ( personId != that.personId )
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
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (countryName != null ? countryName.hashCode() : 0);
        result = 31 * result + workFromYear;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery11{" +
               "personId=" + personId +
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
