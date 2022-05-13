package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LdbcQuery8 extends Operation<List<LdbcQuery8Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 8;
    public static final int DEFAULT_LIMIT = 20;
    public static final String PERSON_ID = "personId";
    public static final String LIMIT = "limit";

    private final long personId;
    private final int limit;

    public LdbcQuery8( long personId, int limit )
    {
        super();
        this.personId = personId;
        this.limit = limit;
    }

    public long getPersonId()
    {
        return personId;
    }

    public int getLimit()
    {
        return limit;
    }


    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personId)
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

        LdbcQuery8 that = (LdbcQuery8) o;

        if ( limit != that.limit )
        { return false; }
        if ( personId != that.personId )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery8{" +
               "personId=" + personId +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery8Result> deserializeResult( String serializedResults ) throws IOException
    {
        List<LdbcQuery8Result> marshaledOperationResult;
        marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery8Result[].class));
        return marshaledOperationResult;
    }
  
    @Override
    public int type()
    {
        return TYPE;
    }
}
