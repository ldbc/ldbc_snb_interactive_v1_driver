package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class LdbcSnbBiQuery18FriendRecommendation extends Operation<List<LdbcSnbBiQuery18FriendRecommendationResult>>
{
    public static final int TYPE = 18;
    public static final String PERSON1_ID = "person1Id";
    public static final String TAG = "tag";
    public static final String LIMIT = "limit";

    private final long person1Id;
    private final String tag;
    private final int limit;

    public LdbcSnbBiQuery18FriendRecommendation(long person1Id, String tag, int limit )
    {
        this.person1Id = person1Id;
        this.tag = tag;
        this.limit = limit;
    }

    public long person1Id()
    {
        return person1Id;
    }

    public String tag() {
        return tag;
    }

    public int limit() {
        return limit;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON1_ID, person1Id)
                .put(TAG, tag)
                .put(LIMIT, limit)
                .build();
    }

    @Override
    public String toString() {
        return "LdbcSnbBiQuery18FriendRecommendation{" +
                "person1Id=" + person1Id +
                ", tag='" + tag + '\'' +
                ", limit=" + limit +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcSnbBiQuery18FriendRecommendation that = (LdbcSnbBiQuery18FriendRecommendation) o;

        if (person1Id != that.person1Id) return false;
        if (limit != that.limit) return false;
        return tag != null ? tag.equals(that.tag) : that.tag == null;
    }

    @Override
    public int hashCode() {
        int result = (int) (person1Id ^ (person1Id >>> 32));
        result = 31 * result + (tag != null ? tag.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery18FriendRecommendationResult> marshalResult( String serializedResults ) throws
            SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery18FriendRecommendationResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long person2Id = ((Number) row.get( 0 )).longValue();
            int mutualFriendCount = ((Number) row.get( 1 )).intValue();
            result.add(
                    new LdbcSnbBiQuery18FriendRecommendationResult( person2Id, mutualFriendCount )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery18FriendRecommendationResult> result =
                (List<LdbcSnbBiQuery18FriendRecommendationResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery18FriendRecommendationResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.person2Id() );
            resultFields.add( row.mutualFriendCount() );
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
