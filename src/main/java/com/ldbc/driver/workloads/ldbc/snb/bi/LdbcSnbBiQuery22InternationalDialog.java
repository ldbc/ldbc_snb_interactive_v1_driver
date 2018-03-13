package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LdbcSnbBiQuery22InternationalDialog extends Operation<List<LdbcSnbBiQuery22InternationalDialogResult>>
{
    public static final int TYPE = 22;
    public static final int DEFAULT_LIMIT = 100;
    public static final String COUNTRY1 = "country1";
    public static final String COUNTRY2 = "country2";
    public static final String LIMIT = "limit";

    private final String country1;
    private final String country2;
    private final int limit;

    public LdbcSnbBiQuery22InternationalDialog( String country1, String country2, int limit )
    {
        this.country1 = country1;
        this.country2 = country2;
        this.limit = limit;
    }

    public String country1()
    {
        return country1;
    }

    public String country2()
    {
        return country2;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(COUNTRY1, country1)
                .put(COUNTRY2, country2)
                .put(LIMIT, limit)
                .build();
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery22InternationalDialog{" +
               "country1='" + country1 + '\'' +
               ", country2='" + country2 + '\'' +
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

        LdbcSnbBiQuery22InternationalDialog that = (LdbcSnbBiQuery22InternationalDialog) o;

        if ( limit != that.limit )
        { return false; }
        if ( country1 != null ? !country1.equals( that.country1 ) : that.country1 != null )
        { return false; }
        return !(country2 != null ? !country2.equals( that.country2 ) : that.country2 != null);

    }

    @Override
    public int hashCode()
    {
        int result = country1 != null ? country1.hashCode() : 0;
        result = 31 * result + (country2 != null ? country2.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery22InternationalDialogResult> marshalResult( String serializedResults ) throws
            SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery22InternationalDialogResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long personId1 = ((Number) row.get( 0 )).longValue();
            long personId2 = ((Number) row.get( 1 )).longValue();
            String city1Name = (String) row.get( 2 );
            int score = ((Number) row.get( 3 )).intValue();
            result.add(
                    new LdbcSnbBiQuery22InternationalDialogResult(
                            personId1,
                            personId2,
                            city1Name,
                            score
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery22InternationalDialogResult> result =
                (List<LdbcSnbBiQuery22InternationalDialogResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery22InternationalDialogResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.personId1() );
            resultFields.add( row.personId2() );
            resultFields.add( row.city1Name() );
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
