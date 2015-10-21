package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery1PostingSummary extends Operation<List<LdbcSnbBiQuery1PostingSummaryResult>>
{
    public static final int TYPE = 1;
    private final long date;

    public LdbcSnbBiQuery1PostingSummary( long date )
    {
        this.date = date;
    }

    public long date()
    {
        return date;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery1PostingSummary{" +
               "date=" + date +
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

        return date == that.date;

    }

    @Override
    public int hashCode()
    {
        return (int) (date ^ (date >>> 32));
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
            int year = ((Number) resultAsList.get( 0 )).intValue();
            boolean isReply = (boolean) resultAsList.get( 1 );
            int messageLengthCategory = ((Number) resultAsList.get( 2 )).intValue();
            long messageCount = ((Number) resultAsList.get( 3 )).longValue();
            long messageLengthMean = ((Number) resultAsList.get( 4 )).longValue();
            long messageLengthSum = ((Number) resultAsList.get( 5 )).longValue();
            float percentOfTotalMessageCount = ((Number) resultAsList.get( 6 )).floatValue();

            results.add(
                    new LdbcSnbBiQuery1PostingSummaryResult(
                            year,
                            isReply,
                            messageLengthCategory,
                            messageCount,
                            messageLengthMean,
                            messageLengthSum,
                            percentOfTotalMessageCount
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
            resultFields.add( row.year() );
            resultFields.add( row.isComment() );
            resultFields.add( row.messageLengthCategory() );
            resultFields.add( row.messageCount() );
            resultFields.add( row.messageLengthMean() );
            resultFields.add( row.messageLengthSum() );
            resultFields.add( row.percentOfTotalMessageCount() );
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
