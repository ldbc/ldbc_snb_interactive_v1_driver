package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * LdbcShortQuery1PersonProfile.java
 * 
 * Interactive workload short read query 1:
 * -- Profile of a person --
 * 
 * Given a start Person, retrieve their first name, last name,
 * birthday, IP address, browser, and city of residence.
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Map;

public class LdbcShortQuery1PersonProfile extends Operation<LdbcShortQuery1PersonProfileResult>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final int TYPE = 101;
    // Parameters used for replacement in queries
    public static final String PERSON_ID = "personId";

    private final long personIdSQ1;

    public LdbcShortQuery1PersonProfile(
        @JsonProperty("personIdSQ1") long personIdSQ1
    )
    {
        this.personIdSQ1 = personIdSQ1;
    }

    public long getPersonIdSQ1()
    {
        return personIdSQ1;
    }


    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personIdSQ1)
                .build();
    }
    @Override
    public LdbcShortQuery1PersonProfileResult deserializeResult( String serializedResults ) throws IOException
    {
        LdbcShortQuery1PersonProfileResult marshaledOperationResult;
        marshaledOperationResult = OBJECT_MAPPER.readValue(serializedResults, LdbcShortQuery1PersonProfileResult.class);
        return marshaledOperationResult;
    }
   
    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcShortQuery1PersonProfile that = (LdbcShortQuery1PersonProfile) o;

        if ( personIdSQ1 != that.personIdSQ1 )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (personIdSQ1 ^ (personIdSQ1 >>> 32));
    }

    @Override
    public String toString()
    {
        return "LdbcShortQuery1PersonProfile{" +
               "personIdSQ1=" + personIdSQ1 +
               '}';
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
