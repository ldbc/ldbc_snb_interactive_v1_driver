package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery2 extends Operation<List<LdbcSnbBiQuery2Result>>
{
    public static final int TYPE = 2;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final long dateA;
    private final long dateB;
    private final int limit;

    public LdbcSnbBiQuery2( long dateA, long dateB, int limit )
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
        return "LdbcSnbBiQuery2{" +
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

        LdbcSnbBiQuery2 that = (LdbcSnbBiQuery2) o;

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
    public List<LdbcSnbBiQuery2Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery2Result> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            String country = (String) row.get( 0 );
            int month = ((Number) row.get( 1 )).intValue();
            String gender = (String) row.get( 2 );
            String tag = (String) row.get( 3 );
            int count = ((Number) row.get( 4 )).intValue();

            result.add(
                    new LdbcSnbBiQuery2Result(
                            country,
                            month,
                            gender,
                            tag,
                            count
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery2Result> result = (List<LdbcSnbBiQuery2Result>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery2Result row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.country() );
            resultFields.add( row.month() );
            resultFields.add( row.gender() );
            resultFields.add( row.tag() );
            resultFields.add( row.count() );
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
