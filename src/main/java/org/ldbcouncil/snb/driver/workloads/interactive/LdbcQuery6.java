package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * LdbcQuery6.java
 * 
 * Interactive workload complex read query 6:
 * -- Tag co-occurrence --
 * 
 * Given a start Person and some Tag, find the other Tags
 * that occur together with this Tag on Posts that were created
 * by start Person’s friends and friends of friends
 * (excluding start Person). Return top 10 Tags, and the count
 * of Posts that were created by these Persons, which contain
 * both this Tag and the given Tag.
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LdbcQuery6 extends Operation<List<LdbcQuery6Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 6;
    public static final int DEFAULT_LIMIT = 10;
    public static final String PERSON_ID = "personIdQ6";
    public static final String TAG_NAME = "tagName";
    public static final String LIMIT = "limit";

    private final long personIdQ6;
    private final String tagName;
    private final int limit;

    public LdbcQuery6(
        @JsonProperty("personIdQ6") long personIdQ6,
        @JsonProperty("tagName")    String tagName,
        @JsonProperty("limit")      int limit
    )
    {
        this.personIdQ6 = personIdQ6;
        this.tagName = tagName;
        this.limit = limit;
    }

    public long getPersonIdQ6()
    {
        return personIdQ6;
    }

    public String getTagName()
    {
        return tagName;
    }

    public int getLimit()
    {
        return limit;
    }


    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personIdQ6)
                .put(TAG_NAME, tagName)
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

        LdbcQuery6 that = (LdbcQuery6) o;

        if ( limit != that.limit )
        { return false; }
        if ( personIdQ6 != that.personIdQ6 )
        { return false; }
        if ( tagName != null ? !tagName.equals( that.tagName ) : that.tagName != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personIdQ6 ^ (personIdQ6 >>> 32));
        result = 31 * result + (tagName != null ? tagName.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery6{" +
               "personIdQ6=" + personIdQ6 +
               ", tagName='" + tagName + '\'' +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery6Result> deserializeResult( String serializedResults ) throws IOException
    {
        List<LdbcQuery6Result> marshaledOperationResult;
        marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery6Result[].class));
        return marshaledOperationResult;
    }
  
    @Override
    public int type()
    {
        return TYPE;
    }
}
