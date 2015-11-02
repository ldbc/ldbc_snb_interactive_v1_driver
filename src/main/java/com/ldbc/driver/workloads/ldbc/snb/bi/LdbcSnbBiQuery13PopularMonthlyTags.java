package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery13PopularMonthlyTags extends Operation<List<LdbcSnbBiQuery13PopularMonthlyTagsResult>>
{
    public static final int TYPE = 13;
    public static final int DEFAULT_LIMIT = 100;
    private final String country;
    private final int limit;

    public LdbcSnbBiQuery13PopularMonthlyTags( String country, int limit )
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
        return "LdbcSnbBiQuery13{" +
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

        LdbcSnbBiQuery13PopularMonthlyTags that = (LdbcSnbBiQuery13PopularMonthlyTags) o;

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
    public List<LdbcSnbBiQuery13PopularMonthlyTagsResult> marshalResult( String serializedResults )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery13PopularMonthlyTagsResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            int year = ((Number) row.get( 0 )).intValue();
            int month = ((Number) row.get( 1 )).intValue();
            List<LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity> tagPopularities = new ArrayList<>();
            for ( List tagPopularity : (List<List>) row.get( 2 ) )
            {
                tagPopularities.add(
                        new LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity(
                                (String) tagPopularity.get( 0 ),
                                (Integer) tagPopularity.get( 1 )
                        )
                );
            }
            result.add(
                    new LdbcSnbBiQuery13PopularMonthlyTagsResult(
                            year,
                            month,
                            tagPopularities
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery13PopularMonthlyTagsResult> result =
                (List<LdbcSnbBiQuery13PopularMonthlyTagsResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery13PopularMonthlyTagsResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.year() );
            resultFields.add( row.month() );
            List<List> tagPopularitiesAsLists = new ArrayList<>();
            for ( LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity tagPopularity : row.tagPopularities() )
            {
                tagPopularitiesAsLists.add( Lists.newArrayList( tagPopularity.tagName(), tagPopularity.popularity() ) );
            }
            resultFields.add( tagPopularitiesAsLists );
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
