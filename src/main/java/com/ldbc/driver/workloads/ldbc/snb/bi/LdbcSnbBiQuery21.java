package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery21 extends Operation<List<LdbcSnbBiQuery21Result>>
{
    public static final int TYPE = 21;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final String country;
    private final int limit;

    public LdbcSnbBiQuery21( String country, int limit )
    {
        this.country = country;
        this.limit = limit;
    }

    public String country()
    {
        return country;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery21{" +
               "country='" + country + '\'' +
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

        LdbcSnbBiQuery21 that = (LdbcSnbBiQuery21) o;

        if ( limit != that.limit )
        { return false; }
        return !(country != null ? !country.equals( that.country ) : that.country != null);

    }

    @Override
    public int hashCode()
    {
        int result = country != null ? country.hashCode() : 0;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery21Result> marshalResult( String serializedResults ) throws
            SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery21Result> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long personId = ((Number) row.get( 0 )).longValue();
            int zombieCount = ((Number) row.get( 1 )).intValue();
            int realCount = ((Number) row.get( 2 )).intValue();
            int score = ((Number) row.get( 3 )).intValue();
            result.add(
                    new LdbcSnbBiQuery21Result(
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
        List<LdbcSnbBiQuery21Result> result = (List<LdbcSnbBiQuery21Result>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery21Result row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.personId() );
            resultFields.add( row.zombieCount() );
            resultFields.add( row.realCount() );
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
