package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LdbcSnbBiQuery16FakeNewsDetection extends Operation<List<LdbcSnbBiQuery16FakeNewsDetectionResult>>
{
    public static final int TYPE = 16;
    public static final String TAG_A = "tagA";
    public static final String DATE_A = "dateA";
    public static final String TAG_B = "tagB";
    public static final String DATE_B = "dateB";
    public static final String MAX_KNOWS_LIMIT = "maxKnowsLimit";
    public static final String LIMIT = "limit";

    private final String tagA;
    private final long dateA;
    private final String tagB;
    private final long dateB;
    private final int maxKnowsLimit;
    private final int limit;

    public LdbcSnbBiQuery16FakeNewsDetection( String tagA, long dateA, String tagB, long dateB, int maxKnowsLimit, int limit )
    {
        this.tagA = tagA;
        this.dateA = dateA;
        this.tagB = tagB;
        this.dateB = dateB;
        this.maxKnowsLimit = maxKnowsLimit;
        this.limit = limit;
    }

    public String tagA() {
        return tagA;
    }

    public long dateA() {
        return dateA;
    }

    public String tagB() {
        return tagB;
    }

    public long dateB() {
        return dateB;
    }

    public int maxKnowsLimit() { return maxKnowsLimit; }

    public int limit() {
        return limit;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(TAG_A, tagA)
                .put(DATE_A, dateA)
                .put(TAG_B, tagB)
                .put(DATE_B, dateB)
                .put(MAX_KNOWS_LIMIT, maxKnowsLimit)
                .put(LIMIT, limit)
                .build();
    }

    @Override
    public String toString() {
        return "LdbcSnbBiQuery16FakeNewsDetection{" +
                "tagA='" + tagA + '\'' +
                ", dateA=" + dateA +
                ", tagB='" + tagB + '\'' +
                ", dateB=" + dateB +
                ", maxKnowsLimit=" + maxKnowsLimit +
                ", limit=" + limit +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcSnbBiQuery16FakeNewsDetection that = (LdbcSnbBiQuery16FakeNewsDetection) o;

        if (dateA != that.dateA) return false;
        if (dateB != that.dateB) return false;
        if (maxKnowsLimit != that.maxKnowsLimit) return false;
        if (limit != that.limit) return false;
        if (tagA != null ? !tagA.equals(that.tagA) : that.tagA != null) return false;
        return tagB != null ? tagB.equals(that.tagB) : that.tagB == null;
    }

    @Override
    public int hashCode() {
        int result = tagA != null ? tagA.hashCode() : 0;
        result = 31 * result + (int) (dateA ^ (dateA >>> 32));
        result = 31 * result + (tagB != null ? tagB.hashCode() : 0);
        result = 31 * result + (int) (dateB ^ (dateB >>> 32));
        result = 31 * result + maxKnowsLimit;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery16FakeNewsDetectionResult> marshalResult( String serializedResults ) throws
            SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery16FakeNewsDetectionResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long personId = ((Number) row.get( 0 )).longValue();
            int messageCountA = ((Number) row.get( 1 )).intValue();
            int messageCountB = ((Number) row.get( 2 )).intValue();
            result.add(
                    new LdbcSnbBiQuery16FakeNewsDetectionResult( personId, messageCountA, messageCountB )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery16FakeNewsDetectionResult> result =
                (List<LdbcSnbBiQuery16FakeNewsDetectionResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery16FakeNewsDetectionResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.personId() );
            resultFields.add( row.messageCountA() );
            resultFields.add( row.messageCountB() );
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
