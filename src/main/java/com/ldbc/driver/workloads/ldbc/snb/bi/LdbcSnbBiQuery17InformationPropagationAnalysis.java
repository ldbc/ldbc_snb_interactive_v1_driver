package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LdbcSnbBiQuery17InformationPropagationAnalysis extends Operation<List<LdbcSnbBiQuery17InformationPropagationAnalysisResult>>
{
    public static final int TYPE = 17;
    public static final String TAG = "tag";
    public static final String DELTA = "delta";
    public static final String LIMIT = "limit";

    private final String tag;
    private final int delta;
    private final int limit;

    public LdbcSnbBiQuery17InformationPropagationAnalysis( String tag, int delta, int limit )
    {
        this.tag = tag;
        this.delta = delta;
        this.limit = limit;
    }

    public String tag() {
        return tag;
    }

    public int delta() {
        return delta;
    }

    public int limit() {
        return limit;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(TAG, tag)
                .put(DELTA, delta)
                .put(LIMIT, limit)
                .build();
    }

    @Override
    public String toString() {
        return "LdbcSnbBiQuery17InformationPropagationAnalysis{" +
                "tag='" + tag + '\'' +
                ", delta=" + delta +
                ", limit=" + limit +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcSnbBiQuery17InformationPropagationAnalysis that = (LdbcSnbBiQuery17InformationPropagationAnalysis) o;

        if (delta != that.delta) return false;
        if (limit != that.limit) return false;
        return tag != null ? tag.equals(that.tag) : that.tag == null;
    }

    @Override
    public int hashCode() {
        int result = tag != null ? tag.hashCode() : 0;
        result = 31 * result + delta;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery17InformationPropagationAnalysisResult> marshalResult( String serializedResults ) throws
            SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery17InformationPropagationAnalysisResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long person1Id = ((Number) row.get( 0 )).longValue();
            int messageCount = ((Number) row.get( 1 )).intValue();
            result.add(
                    new LdbcSnbBiQuery17InformationPropagationAnalysisResult( person1Id, messageCount )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery17InformationPropagationAnalysisResult> result =
                (List<LdbcSnbBiQuery17InformationPropagationAnalysisResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery17InformationPropagationAnalysisResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.person1Id() );
            resultFields.add( row.messageCount() );
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
