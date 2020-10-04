package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LdbcSnbBiQuery5ActivePosters extends Operation<List<LdbcSnbBiQuery5ActivePostersResult>>
{
    public static final int TYPE = 5;
    public static final int DEFAULT_LIMIT = 100;
    public static final String TAG = "tag";
    public static final String LIMIT = "limit";

    private final String tag;
    private final int limit;

    public LdbcSnbBiQuery5ActivePosters( String tag, int limit )
    {
        this.tag = tag;
        this.limit = limit;
    }

    public String tag()
    {
        return tag;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(TAG, tag)
                .put(LIMIT, limit)
                .build();
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery5{" +
               "tag='" + tag + '\'' +
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

        LdbcSnbBiQuery5ActivePosters that = (LdbcSnbBiQuery5ActivePosters) o;

        if ( limit != that.limit )
        { return false; }
        return !(tag != null ? !tag.equals( that.tag ) : that.tag != null);

    }

    @Override
    public int hashCode()
    {
        int result = tag != null ? tag.hashCode() : 0;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery5ActivePostersResult> marshalResult( String serializedResults )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery5ActivePostersResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long personId = ((Number) row.get( 0 )).longValue();
            int replyCount = ((Number) row.get( 1 )).intValue();
            int likeCount = ((Number) row.get( 2 )).intValue();
            int messageCount = ((Number) row.get( 3 )).intValue();
            int score = ((Number) row.get( 4 )).intValue();
            result.add(
                    new LdbcSnbBiQuery5ActivePostersResult(
                            personId,
                            replyCount,
                            likeCount,
                            messageCount,
                            score
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery5ActivePostersResult> result = (List<LdbcSnbBiQuery5ActivePostersResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery5ActivePostersResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.personId() );
            resultFields.add( row.replyCount() );
            resultFields.add( row.likeCount() );
            resultFields.add( row.messageCount() );
            resultFields.add( row.score() );
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
