package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery24 extends Operation<List<LdbcSnbBiQuery24Result>>
{
    public static final int TYPE = 24;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final String tagClass;
    private final int limit;

    public LdbcSnbBiQuery24( String tagClass, int limit )
    {
        this.tagClass = tagClass;
        this.limit = limit;
    }

    public String tagClass()
    {
        return tagClass;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery24{" +
               "tagClass='" + tagClass + '\'' +
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

        LdbcSnbBiQuery24 that = (LdbcSnbBiQuery24) o;

        if ( limit != that.limit )
        { return false; }
        return !(tagClass != null ? !tagClass.equals( that.tagClass ) : that.tagClass != null);

    }

    @Override
    public int hashCode()
    {
        int result = tagClass != null ? tagClass.hashCode() : 0;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery24Result> marshalResult( String serializedResults ) throws
            SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery24Result> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            int year = ((Number) row.get( 0 )).intValue();
            int month = ((Number) row.get( 1 )).intValue();
            String continent = (String) row.get( 2 );
            int postCount = ((Number) row.get( 3 )).intValue();
            result.add(
                    new LdbcSnbBiQuery24Result(
                            year,
                            month,
                            continent,
                            postCount
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery24Result> result = (List<LdbcSnbBiQuery24Result>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery24Result row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.year() );
            resultFields.add( row.month() );
            resultFields.add( row.continent() );
            resultFields.add( row.postCount() );
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
