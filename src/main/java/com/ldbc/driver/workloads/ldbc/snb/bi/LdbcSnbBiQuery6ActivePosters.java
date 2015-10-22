package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery6ActivePosters extends Operation<List<LdbcSnbBiQuery6ActivePostersResult>>
{
    public static final int TYPE = 6;
    public static final int DEFAULT_LIMIT = 100;
    private final String tag;
    private final int limit;

    public LdbcSnbBiQuery6ActivePosters( String tag, int limit )
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
    public String toString()
    {
        return "LdbcSnbBiQuery6{" +
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

        LdbcSnbBiQuery6ActivePosters that = (LdbcSnbBiQuery6ActivePosters) o;

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
    public List<LdbcSnbBiQuery6ActivePostersResult> marshalResult( String serializedResults )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery6ActivePostersResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long personId = ((Number) row.get( 0 )).longValue();
            int postCount = ((Number) row.get( 1 )).intValue();
            int replyCount = ((Number) row.get( 2 )).intValue();
            int likeCount = ((Number) row.get( 3 )).intValue();
            int score = ((Number) row.get( 4 )).intValue();
            result.add(
                    new LdbcSnbBiQuery6ActivePostersResult(
                            personId,
                            postCount,
                            replyCount,
                            likeCount,
                            score
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery6ActivePostersResult> result = (List<LdbcSnbBiQuery6ActivePostersResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery6ActivePostersResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.personId() );
            resultFields.add( row.postCount() );
            resultFields.add( row.replyCount() );
            resultFields.add( row.likeCount() );
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
