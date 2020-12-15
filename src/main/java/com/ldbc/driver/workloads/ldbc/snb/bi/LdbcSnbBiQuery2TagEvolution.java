package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LdbcSnbBiQuery2TagEvolution extends Operation<List<LdbcSnbBiQuery2TagEvolutionResult>>
{
    public static final int TYPE = 2;
    public static final int DEFAULT_LIMIT = 100;
    public static final String YEAR = "year";
    public static final String TAG_CLASS = "tagClass";
    public static final String MONTH = "month";
    public static final String LIMIT = "limit";

    private final int year;
    private final int month;
    private final String tagClass;
    private final int limit;

    public LdbcSnbBiQuery2TagEvolution( int year, int month, String tagClass, int limit )
    {
        this.year = year;
        this.month = month;
        this.tagClass = tagClass;
        this.limit = limit;
    }

    public int year()
    {
        return year;
    }

    public int month()
    {
        return month;
    }

    public String tagClass() {
        return tagClass;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(YEAR, year)
                .put(MONTH, month)
                .put(TAG_CLASS, tagClass)
                .put(LIMIT, limit)
                .build();
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery2TagEvolution{" +
               "year=" + year +
                ", month=" + month +
                ", tagClass=" + tagClass +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcSnbBiQuery2TagEvolution that = (LdbcSnbBiQuery2TagEvolution) o;

        if (year != that.year) return false;
        if (month != that.month) return false;
        if (limit != that.limit) return false;
        return tagClass != null ? tagClass.equals(that.tagClass) : that.tagClass == null;
    }

    @Override
    public int hashCode() {
        int result = year;
        result = 31 * result + month;
        result = 31 * result + (tagClass != null ? tagClass.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery2TagEvolutionResult> marshalResult( String serializedResults )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery2TagEvolutionResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            String tagName = (String) row.get( 0 );
            int countWindow1 = ((Number) row.get( 1 )).intValue();
            int countWindow2 = ((Number) row.get( 2 )).intValue();
            int diff = ((Number) row.get( 3 )).intValue();
            result.add(
                    new LdbcSnbBiQuery2TagEvolutionResult(
                            tagName,
                            countWindow1,
                            countWindow2,
                            diff
                    )
            );
        }

        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery2TagEvolutionResult> result = (List<LdbcSnbBiQuery2TagEvolutionResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery2TagEvolutionResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.tagName() );
            resultFields.add( row.countWindow1() );
            resultFields.add( row.countWindow2() );
            resultFields.add( row.diff() );
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
