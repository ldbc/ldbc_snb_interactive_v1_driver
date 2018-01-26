package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Map;

public class LdbcSnbBiQuery21Zombies extends Operation<List<LdbcSnbBiQuery21ZombiesResult>>
{
    public static final int TYPE = 21;
    public static final int DEFAULT_LIMIT = 100;
    public static final int DEFAULT_DAYS = 30;
    public static final String COUNTRY = "country";
    public static final String END_DATE = "endDate";
    public static final String LIMIT = "limit";

    private final String country;
    private final long endDate;
    private final int limit;

    public LdbcSnbBiQuery21Zombies( String country, long endDate, int limit )
    {
        this.country = country;
        this.endDate = endDate;
        this.limit = limit;
    }

    public String country()
    {
        return country;
    }

    public long endDate()
    {
        return endDate;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(COUNTRY, country)
                .put(END_DATE, endDate)
                .put(LIMIT, limit)
                .build();
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery21Zombies{" +
               "country='" + country + '\'' +
               ", endDate=" + endDate +
               ", limit=" + limit +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LdbcSnbBiQuery21Zombies that = (LdbcSnbBiQuery21Zombies) o;
        return endDate == that.endDate &&
                limit == that.limit &&
                Objects.equals(country, that.country);
    }

    @Override
    public int hashCode() {

        return Objects.hash(country, endDate, limit);
    }

    @Override
    public List<LdbcSnbBiQuery21ZombiesResult> marshalResult( String serializedResults ) throws
            SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery21ZombiesResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long personId = ((Number) row.get( 0 )).longValue();
            int zombieCount = ((Number) row.get( 1 )).intValue();
            int realCount = ((Number) row.get( 2 )).intValue();
            double score = ((Number) row.get( 3 )).doubleValue();
            result.add(
                    new LdbcSnbBiQuery21ZombiesResult(
                            personId,
                            zombieCount,
                            realCount,
                            score
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery21ZombiesResult> result = (List<LdbcSnbBiQuery21ZombiesResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery21ZombiesResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.personId() );
            resultFields.add( row.zombieLikeCount() );
            resultFields.add( row.totalLikeCount() );
            resultFields.add( row.zombieScore() );
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
