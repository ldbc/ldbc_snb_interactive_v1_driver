package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery3 extends Operation<List<LdbcSnbBiQuery3Result>>
{
    public static final int TYPE = 3;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final long dateA;
    private final long dateB;
    private final int limit;

    public LdbcSnbBiQuery3( long dateA, long dateB, int limit )
    {
        this.dateA = dateA;
        this.dateB = dateB;
        this.limit = limit;
    }

    public long dateA()
    {
        return dateA;
    }

    public long dateB()
    {
        return dateB;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery3{" +
               "dateA=" + dateA +
               ", dateB=" + dateB +
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

        LdbcSnbBiQuery3 that = (LdbcSnbBiQuery3) o;

        if ( dateA != that.dateA )
        {
            return false;
        }
        if ( dateB != that.dateB )
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
        int result = (int) (dateA ^ (dateA >>> 32));
        result = 31 * result + (int) (dateB ^ (dateB >>> 32));
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery3Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery3Result> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            String tag = (String) row.get( 0 );
            int countA = ((Number) row.get( 1 )).intValue();
            int countB = ((Number) row.get( 2 )).intValue();
            int difference = ((Number) row.get( 3 )).intValue();
            result.add(
                    new LdbcSnbBiQuery3Result(
                            tag,
                            countA,
                            countB,
                            difference
                    )
            );
        }

        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery3Result> result = (List<LdbcSnbBiQuery3Result>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery3Result row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.tag() );
            resultFields.add( row.countA() );
            resultFields.add( row.countB() );
            resultFields.add( row.difference() );
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
