package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LdbcSnbBiQuery5TopCountryPosters extends Operation<List<LdbcSnbBiQuery5TopCountryPostersResult>>
{
    public static final int TYPE = 5;
    public static final int DEFAULT_LIMIT = 100;
    public static final String COUNTRY = "country";
    public static final String LIMIT = "limit";

    private final String country;
    private final int limit;

    public LdbcSnbBiQuery5TopCountryPosters( String country, int limit )
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
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(COUNTRY, country)
                .put(LIMIT, limit)
                .build();
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery5TopCountryPosters{" +
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

        LdbcSnbBiQuery5TopCountryPosters that = (LdbcSnbBiQuery5TopCountryPosters) o;

        if ( limit != that.limit )
        { return false; }
        return country != null ? country.equals( that.country ) : that.country == null;
    }

    @Override
    public int hashCode()
    {
        int result = country != null ? country.hashCode() : 0;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery5TopCountryPostersResult> marshalResult( String serializedResults )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery5TopCountryPostersResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long personId = ((Number) row.get( 0 )).longValue();
            String personFirstName = (String) row.get( 1 );
            String personLastName = (String) row.get( 2 );
            long personCreationDate = ((Number) row.get( 3 )).longValue();
            int postCount = ((Number) row.get( 4 )).intValue();
            result.add(
                    new LdbcSnbBiQuery5TopCountryPostersResult(
                            personId,
                            personFirstName,
                            personLastName,
                            personCreationDate,
                            postCount
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery5TopCountryPostersResult> result =
                (List<LdbcSnbBiQuery5TopCountryPostersResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery5TopCountryPostersResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.personId() );
            resultFields.add( row.personFirstName() );
            resultFields.add( row.personLastName() );
            resultFields.add( row.personCreationDate() );
            resultFields.add( row.postCount() );
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
