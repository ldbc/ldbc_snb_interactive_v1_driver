package org.ldbcouncil.snb.driver.workloads.interactive.queries;

import java.util.Map;

import org.ldbcouncil.snb.driver.Operation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;


public class LdbcDelete2RemovePostLike extends Operation<LdbcNoResult> {
    public static final int TYPE = 1010;
    public static final String PERSON_ID = "personId";
    public static final String POST_ID = "postId";

    private final long postId;
    private final long personId;

    public LdbcDelete2RemovePostLike(
        @JsonProperty("personId")    long personId,
        @JsonProperty("postId")    long postId
    )
    {
        this.postId = postId;
        this.personId = personId;
    }

    public long getpostId()
    {
        return postId;
    }

    public long getpersonId()
    {
        return personId;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(POST_ID, postId)
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

        LdbcDelete2RemovePostLike that = (LdbcDelete2RemovePostLike) o;

        if ( postId != that.postId )
        { return false; }
        if ( personId != that.personId )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (postId ^ (postId >>> 32));
        result = 31 * result + (int) (personId ^ (personId >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcDelete2RemovePostLike{" +
               "postId=" + postId +
               ", personId=" + personId +
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
