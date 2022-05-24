package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * LdbcQuery1.java
 * 
 * Interactive workload complex read query 1:
 * -- Transitive friends with certain name --
 * 
 * Given a start person, find Persons with a given first name (fristName) that 
 * the start Person is connected to (excluding start Person) by at most 3 steps 
 * via the knows relationships. Return Persons, including the distance (1..3), 
 * summaries of the Persons workplaces and places of study.
 */

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
    // Parameters used for replacement in queries
    public static final String PERSON_ID = "personId";
    public static final String FIRST_NAME = "firstName";
    public static final String LIMIT = "limit";

    private final long personIdQ1;
    private final String firstName;
    private final int limit;

    public LdbcQuery1(
        @JsonProperty("personIdQ1")  long personIdQ1,
        @JsonProperty("firstName") String firstName,
        @JsonProperty("limit")     int limit
    )
    {
        this.personIdQ1 = personIdQ1;
        this.firstName = firstName;
        this.limit = limit;
    }

    public long getPersonIdQ1()
    {
        return personIdQ1;
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
                .put(PERSON_ID, personIdQ1)
                .put(FIRST_NAME, firstName)
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

        LdbcQuery1 that = (LdbcQuery1) o;

        if ( limit != that.limit )
        { return false; }
        if ( personIdQ1 != that.personIdQ1 )
        { return false; }
        if ( firstName != null ? !firstName.equals( that.firstName ) : that.firstName != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personIdQ1 ^ (personIdQ1 >>> 32));
        result = 31 * result + (firstName != null ? firstName.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery1{" +
               "personIdQ1=" + personIdQ1 +
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
