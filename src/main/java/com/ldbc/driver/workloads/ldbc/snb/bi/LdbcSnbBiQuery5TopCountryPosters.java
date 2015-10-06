package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery5TopCountryPosters extends Operation<List<LdbcSnbBiQuery5TopCountryPostersResult>>
{
    public static final int TYPE = 5;
    public static final int DEFAULT_LIMIT = 100;
    public static final int DEFAULT_POPULAR_FORUM_LIMIT = 100;
    private final String country;
    private final int popularForumLimit;
    private final int limit;

    public LdbcSnbBiQuery5TopCountryPosters( String country, int popularForumLimit, int limit )
    {
        this.country = country;
        this.popularForumLimit = popularForumLimit;
        this.limit = limit;
    }

    public String country()
    {
        return country;
    }

    public int popularForumLimit()
    {
        return popularForumLimit;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery5TopCountryPosters{" +
               "country='" + country + '\'' +
               ", popularForumLimit=" + popularForumLimit +
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

        if ( popularForumLimit != that.popularForumLimit )
        { return false; }
        if ( limit != that.limit )
        { return false; }
        return !(country != null ? !country.equals( that.country ) : that.country != null);

    }

    @Override
    public int hashCode()
    {
        int result = country != null ? country.hashCode() : 0;
        result = 31 * result + popularForumLimit;
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
            String firstName = (String) row.get( 1 );
            String lastName = (String) row.get( 2 );
            long creationDate = ((Number) row.get( 3 )).longValue();
            int count = ((Number) row.get( 4 )).intValue();
            result.add(
                    new LdbcSnbBiQuery5TopCountryPostersResult(
                            personId,
                            firstName,
                            lastName,
                            creationDate,
                            count
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
            resultFields.add( row.firstName() );
            resultFields.add( row.lastName() );
            resultFields.add( row.creationDate() );
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
