package org.ldbcouncil.snb.driver.workloads.interactive;

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
    public static final String PERSON_ID = "personIdQ1";

    private final long personIdQ1;

    public LdbcShortQuery1PersonProfile(@JsonProperty("personIdQ1") long personIdQ1 )
    {
        this.personIdQ1 = personIdQ1;
    }

    public long getPersonIdQ1()
    {
        return personIdQ1;
    }


    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personIdQ1)
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

        if ( personIdQ1 != that.personIdQ1 )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (personIdQ1 ^ (personIdQ1 >>> 32));
    }

    @Override
    public String toString()
    {
        return "LdbcShortQuery1PersonProfile{" +
               "personIdQ1=" + personIdQ1 +
               '}';
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
