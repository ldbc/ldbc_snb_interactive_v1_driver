package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LdbcSnbBiQuery19InteractionPathBetweenCities extends Operation<List<LdbcSnbBiQuery19InteractionPathBetweenCitiesResult>>
{
    public static final int TYPE = 19;
    public static final String CITY1_ID = "city1Id";
    public static final String CITY2_ID = "city2Id";

    private final long city1Id;
    private final long city2Id;

    public LdbcSnbBiQuery19InteractionPathBetweenCities( long city1Id, long city2Id )
    {
        this.city1Id = city1Id;
        this.city2Id = city2Id;
    }

    public long city1Id() {
        return city1Id;
    }

    public long city2Id() {
        return city2Id;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(CITY1_ID, city1Id)
                .put(CITY2_ID, city2Id)
                .build();
    }

    @Override
    public String toString() {
        return "LdbcSnbBiQuery19InteractionPathBetweenCities{" +
                "city1Id=" + city1Id +
                ", city2Id=" + city2Id +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcSnbBiQuery19InteractionPathBetweenCities that = (LdbcSnbBiQuery19InteractionPathBetweenCities) o;

        if (city1Id != that.city1Id) return false;
        return city2Id == that.city2Id;
    }

    @Override
    public int hashCode() {
        int result = (int) (city1Id ^ (city1Id >>> 32));
        result = 31 * result + (int) (city2Id ^ (city2Id >>> 32));
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery19InteractionPathBetweenCitiesResult> marshalResult( String serializedResults ) throws
            SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery19InteractionPathBetweenCitiesResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long person1Id = ((Number) row.get( 0 )).longValue();
            long person2Id = ((Number) row.get( 1 )).longValue();
            float totalWeight = ((Number) row.get( 2 )).floatValue();
            result.add(
                    new LdbcSnbBiQuery19InteractionPathBetweenCitiesResult( person1Id, person2Id, totalWeight )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery19InteractionPathBetweenCitiesResult> result =
                (List<LdbcSnbBiQuery19InteractionPathBetweenCitiesResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery19InteractionPathBetweenCitiesResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.person1Id() );
            resultFields.add( row.person2Id() );
            resultFields.add( row.totalWeight() );
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
