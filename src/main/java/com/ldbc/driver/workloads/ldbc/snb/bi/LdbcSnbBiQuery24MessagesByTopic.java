package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery24MessagesByTopic extends Operation<List<LdbcSnbBiQuery24MessagesByTopicResult>>
{
    public static final int TYPE = 24;
    public static final int DEFAULT_LIMIT = 100;
    private final String tagClass;
    private final int limit;

    public LdbcSnbBiQuery24MessagesByTopic( String tagClass, int limit )
    {
        this.tagClass = tagClass;
        this.limit = limit;
    }

    public String tagClass()
    {
        return tagClass;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery24{" +
               "tagClass='" + tagClass + '\'' +
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

        LdbcSnbBiQuery24MessagesByTopic that = (LdbcSnbBiQuery24MessagesByTopic) o;

        if ( limit != that.limit )
        { return false; }
        return !(tagClass != null ? !tagClass.equals( that.tagClass ) : that.tagClass != null);

    }

    @Override
    public int hashCode()
    {
        int result = tagClass != null ? tagClass.hashCode() : 0;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery24MessagesByTopicResult> marshalResult( String serializedResults ) throws
            SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery24MessagesByTopicResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            int messageCount = ((Number) row.get( 0 )).intValue();
            int likeCount = ((Number) row.get( 1 )).intValue();
            int year = ((Number) row.get( 2 )).intValue();
            int month = ((Number) row.get( 3 )).intValue();
            String continent = (String) row.get( 4 );
            result.add(
                    new LdbcSnbBiQuery24MessagesByTopicResult(
                            messageCount,
                            likeCount,
                            year,
                            month,
                            continent
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery24MessagesByTopicResult> result =
                (List<LdbcSnbBiQuery24MessagesByTopicResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery24MessagesByTopicResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.messageCount() );
            resultFields.add( row.likeCount() );
            resultFields.add( row.year() );
            resultFields.add( row.month() );
            resultFields.add( row.continent() );
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
