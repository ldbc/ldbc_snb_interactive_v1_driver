package org.ldbcouncil.snb.driver.workloads.interactive.queries;

import java.util.Map;

import org.ldbcouncil.snb.driver.Operation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;


public class LdbcDelete1RemovePerson extends Operation<LdbcNoResult>{
    public static final int TYPE = 1009;
    public static final String PERSON_ID = "personId";

    private final long personId;

    public LdbcDelete1RemovePerson(
        @JsonProperty("personId")    long personId
    )
    {
        this.personId = personId;
    }

    public long getpersonId()
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
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcDelete1RemovePerson that = (LdbcDelete1RemovePerson) o;

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
        return "LdbcDelete1RemovePerson{" +
               "personId=" + personId +
               '}';
    }

    @Override
    public LdbcNoResult deserializeResult( String serializedResults )
    {
        return LdbcNoResult.INSTANCE;
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
