package org.ldbcouncil.snb.driver.workloads.interactive.queries;

import java.util.Map;

import org.ldbcouncil.snb.driver.Operation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

public class LdbcDelete8RemoveFriendship extends Operation<LdbcNoResult> {
    public static final int TYPE = 1016;
    public static final String PERSON1_ID = "person1Id";
    public static final String PERSON2_ID = "person2Id";

    private final long person1Id;
    private final long person2Id;

    public LdbcDelete8RemoveFriendship(
        @JsonProperty("person1Id")    long person1Id,
        @JsonProperty("person2Id")    long person2Id
    )
    {
        this.person1Id = person1Id;
        this.person2Id = person2Id;
    }

    public long getPerson1Id()
    {
        return person1Id;
    }

    public long getPerson2Id()
    {
        return person2Id;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON1_ID, person1Id)
                .put(PERSON2_ID, person2Id)
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

        if ( person1Id != that.person1Id )
        { return false; }
        if ( person2Id != that.person2Id )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (person1Id ^ (person1Id >>> 32));
        result = 31 * result + (int) (person2Id ^ (person2Id >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcDelete8RemoveFriendship{" +
               "person1Id=" + person1Id +
               ", person2Id=" + person2Id +
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
