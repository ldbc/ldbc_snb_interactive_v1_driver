package org.ldbcouncil.snb.driver.workloads.interactive.queries;

import java.util.Map;

import org.ldbcouncil.snb.driver.Operation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

public class LdbcDelete8RemoveFriendship extends Operation<LdbcNoResult> {
    public static final int TYPE = 1016;
    public static final String PERSON1_ID = "removePerson1Id";
    public static final String PERSON2_ID = "removePerson2Id";

    private final long removePerson1Id;
    private final long removePerson2Id;

    public LdbcDelete8RemoveFriendship(
        @JsonProperty("removePerson1Id")    long removePerson1Id,
        @JsonProperty("removePerson2Id")    long removePerson2Id
    )
    {
        this.removePerson1Id = removePerson1Id;
        this.removePerson2Id = removePerson2Id;
    }

    public long getremovePerson1Id()
    {
        return removePerson1Id;
    }

    public long getremovePerson2Id()
    {
        return removePerson2Id;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON1_ID, removePerson1Id)
                .put(PERSON2_ID, removePerson2Id)
                .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcDelete8RemoveFriendship that = (LdbcDelete8RemoveFriendship) o;

        if ( removePerson1Id != that.removePerson1Id )
        { return false; }
        if ( removePerson2Id != that.removePerson2Id )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (removePerson1Id ^ (removePerson1Id >>> 32));
        result = 31 * result + (int) (removePerson2Id ^ (removePerson2Id >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcDelete8RemoveFriendship{" +
               "removePerson1Id=" + removePerson1Id +
               ", removePerson2Id=" + removePerson2Id +
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
