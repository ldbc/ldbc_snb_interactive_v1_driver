package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LdbcSnbBiQuery14TopThreadInitiators extends Operation<List<LdbcSnbBiQuery14TopThreadInitiatorsResult>>
{
    public static final int TYPE = 14;
    public static final int DEFAULT_LIMIT = 100;
    public static final String START_DATE = "startDate";
    public static final String END_DATE = "endDate";
    public static final String LIMIT = "limit";

    private final long startDate;
    private final long endDate;
    private final int limit;

    public LdbcSnbBiQuery14TopThreadInitiators( long startDate, long endDate, int limit )
    {
        this.startDate = startDate;
        this.endDate = endDate;
        this.limit = limit;
    }

    public long startDate()
    {
        return startDate;
    }

    public long endDate()
    {
        return endDate;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(START_DATE, startDate)
                .put(END_DATE, endDate)
                .put(LIMIT, limit)
                .build();
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery14TopThreadInitiators{" +
               "startDate=" + startDate +
               ", endDate=" + endDate +
               ", limit=" + limit +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery14TopThreadInitiators that = (LdbcSnbBiQuery14TopThreadInitiators) o;

        if ( startDate != that.startDate)
        { return false; }
        if ( endDate != that.endDate )
        { return false; }
        return limit == that.limit;

    }

    @Override
    public int hashCode()
    {
        int result = (int) (startDate ^ (startDate >>> 32));
        result = 31 * result + (int) (endDate ^ (endDate >>> 32));
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery14TopThreadInitiatorsResult> marshalResult( String serializedResults )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery14TopThreadInitiatorsResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long personId = ((Number) row.get( 0 )).longValue();
            String personFirstName = (String) row.get( 1 );
            String personLastName = (String) row.get( 2 );
            int threadCount = ((Number) row.get( 3 )).intValue();
            int messageCount = ((Number) row.get( 4 )).intValue();
            result.add(
                    new LdbcSnbBiQuery14TopThreadInitiatorsResult(
                            personId,
                            personFirstName,
                            personLastName,
                            threadCount,
                            messageCount
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery14TopThreadInitiatorsResult> result =
                (List<LdbcSnbBiQuery14TopThreadInitiatorsResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery14TopThreadInitiatorsResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.personId() );
            resultFields.add( row.personFirstName() );
            resultFields.add( row.personLastName() );
            resultFields.add( row.threadCount() );
            resultFields.add( row.messageCount() );
            resultsFields.add( resultFields );
        }
        return SerializationUtil.toJson( resultsFields );
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
