package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery1 extends Operation<List<LdbcSnbBiQuery1Result>>
{
    public static final int TYPE = 1;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final long date;
    private final int limit;

    public LdbcSnbBiQuery1( long date, int limit )
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
        return "LdbcSnbBiQuery1{" +
               "date=" + date +
               ", limit=" + limit +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        LdbcSnbBiQuery1 that = (LdbcSnbBiQuery1) o;

        if ( date != that.date )
        {
            return false;
        }
        if ( limit != that.limit )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (date ^ (date >>> 32));
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery1Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery1Result> results = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> resultAsList = resultsAsList.get( i );
            int year = ((Number) resultAsList.get( 0 )).intValue();
            boolean isReply = (boolean) resultAsList.get( 1 );
            int size = ((Number) resultAsList.get( 2 )).intValue();
            int count = ((Number) resultAsList.get( 3 )).intValue();
            int averageLength = ((Number) resultAsList.get( 4 )).intValue();
            int total = ((Number) resultAsList.get( 5 )).intValue();
            double percent = ((Number) resultAsList.get( 6 )).doubleValue();

            results.add(
                    new LdbcSnbBiQuery1Result(
                            year,
                            isReply,
                            size,
                            count,
                            averageLength,
                            total,
                            percent
                    )
            );
        }
        return results;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery1Result> result = (List<LdbcSnbBiQuery1Result>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery1Result row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.year() );
            resultFields.add( row.isReply() );
            resultFields.add( row.size() );
            resultFields.add( row.count() );
            resultFields.add( row.averageLength() );
            resultFields.add( row.total() );
            resultFields.add( row.percent() );
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
