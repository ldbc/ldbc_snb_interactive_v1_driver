package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery14 extends Operation<List<LdbcSnbBiQuery14Result>>
{
    public static final int TYPE = 14;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final long date;
    private final int limit;

    public LdbcSnbBiQuery14( long date, int limit )
    {
        this.date = date;
        this.limit = limit;
    }

    public long date()
    {
        return date;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery14{" +
               "date=" + date +
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

        LdbcSnbBiQuery14 that = (LdbcSnbBiQuery14) o;

        if ( date != that.date )
        { return false; }
        return limit == that.limit;

    }

    @Override
    public int hashCode()
    {
        int result = (int) (date ^ (date >>> 32));
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery14Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery14Result> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long personId = ((Number) row.get( 0 )).longValue();
            String firstName = (String) row.get( 1 );
            String lastName = (String) row.get( 2 );
            int count = ((Number) row.get( 3 )).intValue();
            int threadCount = ((Number) row.get( 4 )).intValue();
            result.add(
                    new LdbcSnbBiQuery14Result(
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
        List<LdbcSnbBiQuery14Result> result = (List<LdbcSnbBiQuery14Result>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery14Result row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.personId() );
            resultFields.add( row.firstName() );
            resultFields.add( row.lastName() );
            resultFields.add( row.count() );
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
