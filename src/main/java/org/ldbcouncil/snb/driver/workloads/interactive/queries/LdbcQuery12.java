package org.ldbcouncil.snb.driver.workloads.interactive.queries;
/**
 * LdbcQuery12.java
 * 
 * Interactive workload complex read query 12:
 * -- Expert search --
 * 
 * Given a start Person, find the Comments that this Person’s friends
 * made in reply to Posts, considering only those Comments that are direct
 * (single-hop) replies to Posts, not the transitive (multi-hop) ones. Only
 * consider Posts with a Tag in a given TagClass or in a descendent of that
 * TagClass. Count the number of these reply Comments, and collect the Tags
 * that were attached to the Posts they replied to, but only collect Tags
 * with the given TagClass or with a descendant of that TagClass. Return
 * Persons with at least one reply, the reply count, and the collection of Tags
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcOperation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LdbcQuery12 extends LdbcOperation<List<LdbcQuery12Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 12;
    public static final int DEFAULT_LIMIT = 20;
    // Parameters used for replacement in queries
    public static final String PERSON_ID = "personId";
    public static final String TAG_CLASS_NAME = "tagClassName";
    public static final String LIMIT = "limit";

    private final long personIdQ12;
    private final String tagClassName;
    private final int limit;

    public LdbcQuery12(
        @JsonProperty("personIdQ12")  long personIdQ12,
        @JsonProperty("tagClassName") String tagClassName,
        @JsonProperty("limit")        int limit
    )
    {
        this.personIdQ12 = personIdQ12;
        this.tagClassName = tagClassName;
        this.limit = limit;
    }

    public LdbcQuery12( LdbcQuery12 query )
    {
        this.personIdQ12 = query.getPersonIdQ12();
        this.tagClassName = query.getTagClassName();
        this.limit = query.getLimit();
    }

    public long getPersonIdQ12()
    {
        return personIdQ12;
    }

    public String getTagClassName()
    {
        return tagClassName;
    }

    public int getLimit()
    {
        return limit;
    }

    @Override
    public LdbcQuery12 newInstance(){
        return new LdbcQuery12(this);
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personIdQ12)
                .put(TAG_CLASS_NAME, tagClassName)
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

        LdbcQuery12 that = (LdbcQuery12) o;

        if ( limit != that.limit )
        { return false; }
        if ( personIdQ12 != that.personIdQ12 )
        { return false; }
        if ( tagClassName != null ? !tagClassName.equals( that.tagClassName ) : that.tagClassName != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personIdQ12 ^ (personIdQ12 >>> 32));
        result = 31 * result + (tagClassName != null ? tagClassName.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery12{" +
               "personIdQ12=" + personIdQ12 +
               ", tagClassName='" + tagClassName + '\'' +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery12Result> deserializeResult( String serializedResults ) throws IOException
    {
        List<LdbcQuery12Result> marshaledOperationResult;
        marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery12Result[].class));
        return marshaledOperationResult;
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
