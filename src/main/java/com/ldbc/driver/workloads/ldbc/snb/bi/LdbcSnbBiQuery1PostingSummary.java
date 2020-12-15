package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LdbcSnbBiQuery1PostingSummary extends Operation<List<LdbcSnbBiQuery1PostingSummaryResult>>
{
    public static final int TYPE = 1;
    public static final String DATETIME = "datetime";

    private final long datetime;

    public LdbcSnbBiQuery1PostingSummary( long datetime )
    {
        this.datetime = datetime;
    }

    public long datetime()
    {
        return datetime;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(DATETIME, datetime)
                .build();
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery1PostingSummary{" +
               "datetime=" + datetime +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery1PostingSummary that = (LdbcSnbBiQuery1PostingSummary) o;

        return datetime == that.datetime;

    }

    @Override
    public int hashCode()
    {
        return (int) (datetime ^ (datetime >>> 32));
    }

    @Override
    public List<LdbcSnbBiQuery1PostingSummaryResult> marshalResult( String serializedResults )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery1PostingSummaryResult> results = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> resultAsList = resultsAsList.get( i );
            int messageYear = ((Number) resultAsList.get( 0 )).intValue();
            boolean isComment = (boolean) resultAsList.get( 1 );
            int lengthCategory = ((Number) resultAsList.get( 2 )).intValue();
            long messageCount = ((Number) resultAsList.get( 3 )).longValue();
            long averageMessageLength = ((Number) resultAsList.get( 4 )).longValue();
            long sumMessageLength = ((Number) resultAsList.get( 5 )).longValue();
            float percentageOfMessages = ((Number) resultAsList.get( 6 )).floatValue();

            results.add(
                    new LdbcSnbBiQuery1PostingSummaryResult(
                            messageYear,
                            isComment,
                            lengthCategory,
                            messageCount,
                            averageMessageLength,
                            sumMessageLength,
                            percentageOfMessages
                    )
            );
        }
        return results;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery1PostingSummaryResult> result = (List<LdbcSnbBiQuery1PostingSummaryResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery1PostingSummaryResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.messageYear() );
            resultFields.add( row.isComment() );
            resultFields.add( row.lengthCategory() );
            resultFields.add( row.messageCount() );
            resultFields.add( row.averageMessageLength() );
            resultFields.add( row.sumMessageLength() );
            resultFields.add( row.percentOfMessages() );
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
