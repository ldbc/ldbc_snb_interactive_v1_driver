package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery14TopThreadInitiators extends Operation<List<LdbcSnbBiQuery14TopThreadInitiatorsResult>>
{
    public static final int TYPE = 14;
    public static final int DEFAULT_LIMIT = 100;
    private final long beginDate;
    private final long endDate;
    private final int limit;

    public LdbcSnbBiQuery14TopThreadInitiators( long beginDate, long endDate, int limit )
    {
        this.beginDate = beginDate;
        this.endDate = endDate;
        this.limit = limit;
    }

    public long beginDate()
    {
        return beginDate;
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
    public String toString()
    {
        return "LdbcSnbBiQuery14TopThreadInitiators{" +
               "beginDate=" + beginDate +
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

        if ( beginDate != that.beginDate )
        { return false; }
        if ( endDate != that.endDate )
        { return false; }
        return limit == that.limit;

    }

    @Override
    public int hashCode()
    {
        int result = (int) (beginDate ^ (beginDate >>> 32));
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
            String firstName = (String) row.get( 1 );
            String lastName = (String) row.get( 2 );
            int count = ((Number) row.get( 3 )).intValue();
            int threadCount = ((Number) row.get( 4 )).intValue();
            result.add(
                    new LdbcSnbBiQuery14TopThreadInitiatorsResult(
                            personId,
                            firstName,
                            lastName,
                            count,
                            threadCount
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
            resultFields.add( row.firstName() );
            resultFields.add( row.lastName() );
            resultFields.add( row.messageCount() );
            resultFields.add( row.threadCount() );
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
