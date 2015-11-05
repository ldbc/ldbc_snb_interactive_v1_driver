package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery20HighLevelTopics extends Operation<List<LdbcSnbBiQuery20HighLevelTopicsResult>>
{
    public static final int TYPE = 20;
    public static final int DEFAULT_LIMIT = 100;
    private final List<String> tagClasses;
    private final int limit;

    public LdbcSnbBiQuery20HighLevelTopics( List<String> tagClasses, int limit )
    {
        this.tagClasses = tagClasses;
        this.limit = limit;
    }

    public List<String> tagClasses()
    {
        return tagClasses;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery20HighLevelTopics{" +
               "tagClasses=" + tagClasses +
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

        LdbcSnbBiQuery20HighLevelTopics that = (LdbcSnbBiQuery20HighLevelTopics) o;

        if ( limit != that.limit )
        { return false; }
        return !(tagClasses != null ? !tagClasses.equals( that.tagClasses ) : that.tagClasses != null);

    }

    @Override
    public int hashCode()
    {
        int result = tagClasses != null ? tagClasses.hashCode() : 0;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery20HighLevelTopicsResult> marshalResult( String serializedResults )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery20HighLevelTopicsResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            String tagClass = (String) row.get( 0 );
            int count = ((Number) row.get( 1 )).intValue();
            result.add(
                    new LdbcSnbBiQuery20HighLevelTopicsResult(
                            tagClass,
                            count
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery20HighLevelTopicsResult> result =
                (List<LdbcSnbBiQuery20HighLevelTopicsResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery20HighLevelTopicsResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.tagClass() );
            resultFields.add( row.count() );
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
