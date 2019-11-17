package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LdbcSnbBiQuery10TagPerson extends Operation<List<LdbcSnbBiQuery10TagPersonResult>>
{
    public static final int TYPE = 10;
    public static final int DEFAULT_LIMIT = 100;
    public static final String TAG = "tag";
    public static final String DATE = "date";
    public static final String LIMIT = "limit";

    private final String tag;
    private final long date;
    private final int limit;

    public LdbcSnbBiQuery10TagPerson( String tag, long date, int limit )
    {
        this.tag = tag;
        this.date = date;
        this.limit = limit;
    }

    public String tag()
    {
        return tag;
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
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(TAG, tag)
                .put(DATE, date)
                .put(LIMIT, limit)
                .build();
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery10TagPersonResult{" +
               "tag='" + tag + '\'' +
               "date='" + date + '\'' +
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

        LdbcSnbBiQuery10TagPerson that = (LdbcSnbBiQuery10TagPerson) o;

        if ( date != that.date )
        { return false; }
        if ( limit != that.limit )
        { return false; }
        return tag != null ? tag.equals( that.tag ) : that.tag == null;
    }

    @Override
    public int hashCode()
    {
        int result = tag != null ? tag.hashCode() : 0;
        result = 31 * result + (int) (date ^ (date >>> 32));
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery10TagPersonResult> marshalResult( String serializedResults )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery10TagPersonResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long personId = ((Number) row.get( 0 )).longValue();
            int score = ((Number) row.get( 1 )).intValue();
            int friendsScore = ((Number) row.get( 2 )).intValue();
            result.add(
                    new LdbcSnbBiQuery10TagPersonResult(
                            personId,
                            score,
                            friendsScore
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery10TagPersonResult> result = (List<LdbcSnbBiQuery10TagPersonResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery10TagPersonResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.personId() );
            resultFields.add( row.score() );
            resultFields.add( row.friendsScore() );
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
