package org.ldbcouncil.snb.driver.workloads.interactive.queries;

import java.util.Map;

import org.ldbcouncil.snb.driver.workloads.interactive.LdbcOperation;
import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LdbcDelete1RemovePerson extends LdbcOperation<LdbcNoResult>{
    public static final int TYPE = 1009;
    public static final String PERSON_ID = "personId";

    private final long removePersonIdD1;

    public LdbcDelete1RemovePerson(
        @JsonProperty("removePersonIdD1")    long removePersonIdD1
    )
    {
        this.removePersonIdD1 = removePersonIdD1;
    }

    public long getremovePersonIdD1()
    {
        return removePersonIdD1;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, removePersonIdD1)
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

        if ( removePersonIdD1 != that.removePersonIdD1 )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (removePersonIdD1 ^ (removePersonIdD1 >>> 32));
    }

    @Override
    public String toString()
    {
        return "LdbcDelete1RemovePerson{" +
               "removePersonIdD1=" + removePersonIdD1 +
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
