package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Map;

public class LdbcShortQuery1PersonProfile extends Operation<LdbcShortQuery1PersonProfileResult>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final int TYPE = 101;
    public static final String PERSON_ID = "personId";

    private final long personId;

    public LdbcShortQuery1PersonProfile( long personId )
    {
        this.personId = personId;
    }

    public long getPersonId()
    {
        return personId;
    }


    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personId)
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

        if ( personId != that.personId )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (personId ^ (personId >>> 32));
    }

    @Override
    public String toString()
    {
        return "LdbcShortQuery1PersonProfile{" +
               "personId=" + personId +
               '}';
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}