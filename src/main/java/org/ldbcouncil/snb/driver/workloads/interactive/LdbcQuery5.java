package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class LdbcQuery5 extends Operation<List<LdbcQuery5Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 5;
    public static final int DEFAULT_LIMIT = 20;
    public static final String PERSON_ID = "personId";
    public static final String MIN_DATE = "minDate";
    public static final String LIMIT = "limit";

    private final long personId;
    private final Date minDate;
    private final int limit;

    public LdbcQuery5( long personId, Date minDate, int limit )
    {
        super();
        this.personId = personId;
        this.minDate = minDate;
        this.limit = limit;
    }

    public long getPersonId()
    {
        return personId;
    }

    public Date getMinDate()
    {
        return minDate;
    }

    public int getLimit()
    {
        return limit;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personId)
                .put(MIN_DATE, minDate)
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

        LdbcQuery5 that = (LdbcQuery5) o;

        if ( limit != that.limit )
        { return false; }
        if ( personId != that.personId )
        { return false; }
        if ( minDate != null ? !minDate.equals( that.minDate ) : that.minDate != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (minDate != null ? minDate.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery5{" +
               "personId=" + personId +
               ", minDate=" + minDate +
               ", limit=" + limit +
               '}';
    }

    @Override
    public List<LdbcQuery5Result> deserializeResult( String serializedResults ) throws IOException
    {
        List<LdbcQuery5Result> marshaledOperationResult;
        marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery5Result[].class));
        return marshaledOperationResult;
    }
  
    @Override
    public int type()
    {
        return TYPE;
    }
}
