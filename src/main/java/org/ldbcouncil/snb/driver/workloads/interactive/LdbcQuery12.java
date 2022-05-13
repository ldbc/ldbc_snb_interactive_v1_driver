package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LdbcQuery12 extends Operation<List<LdbcQuery12Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 12;
    public static final int DEFAULT_LIMIT = 20;
    public static final String PERSON_ID = "personId";
    public static final String TAG_CLASS_NAME = "tagClassName";
    public static final String LIMIT = "limit";

    private final long personId;
    private final String tagClassName;
    private final int limit;

    public LdbcQuery12(
        @JsonProperty("personId") long personId,
        @JsonProperty("tagClassName") String tagClassName,
        @JsonProperty("limit") int limit
    )
    {
        this.personId = personId;
        this.tagClassName = tagClassName;
        this.limit = limit;
    }

    public long getPersonId()
    {
        return personId;
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
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personId)
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
        if ( personId != that.personId )
        { return false; }
        if ( tagClassName != null ? !tagClassName.equals( that.tagClassName ) : that.tagClassName != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (tagClassName != null ? tagClassName.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery12{" +
               "personId=" + personId +
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
