package org.ldbcouncil.snb.driver.workloads.interactive;

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
    public static final String PERSON_ID = "personId";
    public static final String TAG_NAME = "tagName";
    public static final String LIMIT = "limit";

    private final long personId;
    private final String tagName;
    private final int limit;

    public LdbcQuery6(
        @JsonProperty("personId") long personId,
        @JsonProperty("tagName") String tagName,
        @JsonProperty("limit") int limit )
    {
        this.personId = personId;
        this.tagName = tagName;
        this.limit = limit;
    }

    public long getPersonId()
    {
        return personId;
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
                .put(PERSON_ID, personId)
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
        if ( personId != that.personId )
        { return false; }
        if ( tagName != null ? !tagName.equals( that.tagName ) : that.tagName != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (tagName != null ? tagName.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery6{" +
               "personId=" + personId +
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
