package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LdbcQuery10 extends Operation<List<LdbcQuery10Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 10;
    public static final int DEFAULT_LIMIT = 10;
    public static final String PERSON_ID = "personId";
    public static final String MONTH = "month";
    public static final String LIMIT = "limit";

    private final long personId;
    private final int month;
    private final int limit;

    public LdbcQuery10( long personId, int month, int limit )
    {
        this.personId = personId;
        this.month = month;
        this.limit = limit;
    }

    public long getPersonId()
    {
        return personId;
    }

    public int getMonth()
    {
        return month;
    }

    public int getLimit()
    {
        return limit;
    }


    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personId)
                .put(MONTH, month)
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

        LdbcQuery10 that = (LdbcQuery10) o;

        if ( limit != that.limit )
        { return false; }
        if ( month != that.month )
        { return false; }
        if ( personId != that.personId )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + month;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery10{" +
               "personId=" + personId +
               ", month=" + month +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery10Result> deserializeResult( String serializedResults ) throws IOException
    {
        List<LdbcQuery10Result> marshaledOperationResult;
        marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery10Result[].class));
        return marshaledOperationResult;
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}