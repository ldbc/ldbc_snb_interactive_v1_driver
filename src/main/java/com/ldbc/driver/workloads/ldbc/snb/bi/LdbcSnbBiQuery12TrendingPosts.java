package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery12TrendingPosts extends Operation<List<LdbcSnbBiQuery12TrendingPostsResult>>
{
    public static final int TYPE = 12;
    public static final int DEFAULT_LIMIT = 100;
    private final long date;
    private final int likeCount;
    private final int limit;

    public LdbcSnbBiQuery12TrendingPosts( long date, int likeCount, int limit )
    {
        this.date = date;
        this.likeCount = likeCount;
        this.limit = limit;
    }

    public long date()
    {
        return date;
    }

    public int likeCount()
    {
        return likeCount;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery12TrendingPosts{" +
               "date=" + date +
               ", likeCount=" + likeCount +
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

        LdbcSnbBiQuery12TrendingPosts that = (LdbcSnbBiQuery12TrendingPosts) o;

        if ( date != that.date )
        { return false; }
        if ( likeCount != that.likeCount )
        { return false; }
        return limit == that.limit;

    }

    @Override
    public int hashCode()
    {
        int result = (int) (date ^ (date >>> 32));
        result = 31 * result + likeCount;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery12TrendingPostsResult> marshalResult( String serializedResults )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery12TrendingPostsResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long postId = ((Number) row.get( 0 )).longValue();
            String firstName = (String) row.get( 1 );
            String lastName = (String) row.get( 2 );
            long creationDate = ((Number) row.get( 3 )).longValue();
            int count = ((Number) row.get( 4 )).intValue();
            result.add(
                    new LdbcSnbBiQuery12TrendingPostsResult(
                            postId,
                            firstName,
                            lastName,
                            creationDate,
                            count
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery12TrendingPostsResult> result = (List<LdbcSnbBiQuery12TrendingPostsResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery12TrendingPostsResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.messageId() );
            resultFields.add( row.firstName() );
            resultFields.add( row.lastName() );
            resultFields.add( row.creationDate() );
            resultFields.add( row.likeCount() );
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
