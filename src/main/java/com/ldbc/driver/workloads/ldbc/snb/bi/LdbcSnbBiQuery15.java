package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery15 extends Operation<List<LdbcSnbBiQuery15Result>>
{
    public static final int TYPE = 15;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final String country;
    private final int limit;

    public LdbcSnbBiQuery15( String country, int limit )
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
        return "LdbcSnbBiQuery15{" +
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

        LdbcSnbBiQuery15 that = (LdbcSnbBiQuery15) o;

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
    public List<LdbcSnbBiQuery15Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery15Result> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long personId = ((Number) row.get( 0 )).longValue();
            int count = ((Number) row.get( 1 )).intValue();
            result.add(
                    new LdbcSnbBiQuery15Result(
                            personId,
                            count
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery15Result> result = (List<LdbcSnbBiQuery15Result>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery15Result row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.personId() );
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
