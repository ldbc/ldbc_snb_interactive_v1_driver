package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery20 extends Operation<List<LdbcSnbBiQuery20Result>>
{
    public static final int TYPE = 20;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final int limit;

    public LdbcSnbBiQuery20( int limit )
    {
        this.limit = limit;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery20{" +
               "limit=" + limit +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery20 that = (LdbcSnbBiQuery20) o;

        return limit == that.limit;

    }

    @Override
    public int hashCode()
    {
        return limit;
    }

    @Override
    public List<LdbcSnbBiQuery20Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery20Result> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            String tagClass = (String) row.get( 0 );
            int count = ((Number) row.get( 1 )).intValue();
            result.add(
                    new LdbcSnbBiQuery20Result(
                            tagClass,
                            count
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery20Result> result = (List<LdbcSnbBiQuery20Result>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery20Result row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.tagClass() );
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
