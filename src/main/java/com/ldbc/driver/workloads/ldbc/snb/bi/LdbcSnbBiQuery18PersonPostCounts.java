package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery18PersonPostCounts extends Operation<List<LdbcSnbBiQuery18PersonPostCountsResult>>
{
    public static final int TYPE = 18;
    public static final int DEFAULT_LIMIT = 100;
    private final long date;
    private final int limit;

    public LdbcSnbBiQuery18PersonPostCounts( long date, int limit )
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
        return "LdbcSnbBiQuery18{" +
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

        LdbcSnbBiQuery18PersonPostCounts that = (LdbcSnbBiQuery18PersonPostCounts) o;

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
    public List<LdbcSnbBiQuery18PersonPostCountsResult> marshalResult( String serializedResults ) throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery18PersonPostCountsResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            int postCount = ((Number) row.get( 0 )).intValue();
            int count = ((Number) row.get( 1 )).intValue();
            result.add(
                    new LdbcSnbBiQuery18PersonPostCountsResult(
                            postCount,
                            count
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery18PersonPostCountsResult> result = (List<LdbcSnbBiQuery18PersonPostCountsResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery18PersonPostCountsResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.messageCount() );
            resultFields.add( row.personCount() );
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
