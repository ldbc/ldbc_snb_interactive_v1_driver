package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LdbcSnbBiQuery12TrendingPosts extends Operation<List<LdbcSnbBiQuery12TrendingPostsResult>>
{
    public static final int TYPE = 12;
    public static final int DEFAULT_LIMIT = 100;
    public static final String DATE = "date";
    public static final String LIKE_THRESHOLD = "likeThreshold";
    public static final String LIMIT = "limit";

    private final long date;
    private final int likeThreshold;
    private final int limit;

    public LdbcSnbBiQuery12TrendingPosts( long date, int likeThreshold, int limit )
    {
        this.date = date;
        this.likeThreshold = likeThreshold;
        this.limit = limit;
    }

    public long date()
    {
        return date;
    }

    public int likeThreshold()
    {
        return likeThreshold;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(DATE, date)
                .put(LIKE_THRESHOLD, likeThreshold)
                .put(LIMIT, limit)
                .build();
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery12TrendingPosts{" +
               "date=" + date +
               ", likeThreshold=" + likeThreshold +
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
        if ( likeThreshold != that.likeThreshold )
        { return false; }
        return limit == that.limit;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (date ^ (date >>> 32));
        result = 31 * result + likeThreshold;
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
            long messageId = ((Number) row.get( 0 )).longValue();
            long messageCreationDate = ((Number) row.get( 1 )).longValue();
            String creatorFirstName = (String) row.get( 2 );
            String creatorLastName = (String) row.get( 3 );
            int likeCount = ((Number) row.get( 4 )).intValue();
            result.add(
                    new LdbcSnbBiQuery12TrendingPostsResult(
                            messageId,
                            messageCreationDate,
                            creatorFirstName,
                            creatorLastName,
                            likeCount
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
            resultFields.add( row.messageCreationDate() );
            resultFields.add( row.creatorFirstName() );
            resultFields.add( row.creatorLastName() );
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
