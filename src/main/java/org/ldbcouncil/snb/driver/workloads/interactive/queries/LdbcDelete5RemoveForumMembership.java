package org.ldbcouncil.snb.driver.workloads.interactive.queries;

import java.util.Map;

import org.ldbcouncil.snb.driver.workloads.interactive.LdbcOperation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

public class LdbcDelete5RemoveForumMembership extends LdbcOperation<LdbcNoResult> {
    public static final int TYPE = 1013;
    public static final String FORUM_ID = "forumId";
    public static final String PERSON_ID = "personId";

    private final long removeForumIdD5;
    private final long removePersonIdD5;

    public LdbcDelete5RemoveForumMembership(
        @JsonProperty("removeForumIdD5")    long removeForumIdD5,
        @JsonProperty("removePersonIdD5")    long removePersonIdD5
    )
    {
        this.removeForumIdD5 = removeForumIdD5;
        this.removePersonIdD5 = removePersonIdD5;
    }

    public long getremoveForumIdD5()
    {
        return removeForumIdD5;
    }

    public long getremovePersonIdD5()
    {
        return removePersonIdD5;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(FORUM_ID, removeForumIdD5)
                .put(PERSON_ID, removePersonIdD5)
                .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcDelete5RemoveForumMembership that = (LdbcDelete5RemoveForumMembership) o;

        if ( removeForumIdD5 != that.removeForumIdD5 )
        { return false; }
        if ( removePersonIdD5 != that.removePersonIdD5 )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (removeForumIdD5 ^ (removeForumIdD5 >>> 32));
        result = 31 * result + (int) (removePersonIdD5 ^ (removePersonIdD5 >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcDelete5RemoveForumMembership{" +
               "removeForumIdD5=" + removeForumIdD5 +
               ", removePersonIdD5=" + removePersonIdD5 +
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
