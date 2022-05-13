package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LdbcQuery1 extends Operation<List<LdbcQuery1Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 1;
    public static final int DEFAULT_LIMIT = 20;
    public static final String PERSON_ID = "personId";
    public static final String FIRST_NAME = "firstName";
    public static final String LIMIT = "limit";

    private final long personId;
    private final String firstName;
    private final int limit;

    public LdbcQuery1(
        @JsonProperty("personId") long personId,
        @JsonProperty("firstName") String firstName,
        @JsonProperty("limit") int limit )
    {
        this.personId = personId;
        this.firstName = firstName;
        this.limit = limit;
}

    public long getPersonId()
    {
        return personId;
    }

    public String getFirstName()
    {
        return firstName;
    }

    public int getLimit()
    {
        return limit;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, "getPersonId")
                .put(FIRST_NAME, "getFirstName")
                .put(LIMIT, "getLimit")
                .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcQuery1 that = (LdbcQuery1) o;

        if ( limit != that.limit )
        { return false; }
        if ( personId != that.personId )
        { return false; }
        if ( firstName != null ? !firstName.equals( that.firstName ) : that.firstName != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (firstName != null ? firstName.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery1{" +
               "personId=" + personId +
               ", firstName='" + firstName + '\'' +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery1Result> deserializeResult( String serializedResults ) throws IOException
    {
        List<LdbcQuery1Result> marshaledOperationResult;
        marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery1Result[].class));
        return marshaledOperationResult;
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
