package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery4PopularCountryTopics extends Operation<List<LdbcSnbBiQuery4PopularCountryTopicsResult>>
{
    public static final int TYPE = 4;
    public static final int DEFAULT_LIMIT = 20;
    private final String tagClass;
    private final String country;
    private final int limit;

    public LdbcSnbBiQuery4PopularCountryTopics( String tagClass, String country, int limit )
    {
        this.tagClass = tagClass;
        this.country = country;
        this.limit = limit;
    }

    public String tagClass()
    {
        return tagClass;
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
        return "LdbcSnbBiQuery4{" +
               "tagClass='" + tagClass + '\'' +
               ", country='" + country + '\'' +
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

        LdbcSnbBiQuery4PopularCountryTopics that = (LdbcSnbBiQuery4PopularCountryTopics) o;

        if ( limit != that.limit )
        { return false; }
        if ( tagClass != null ? !tagClass.equals( that.tagClass ) : that.tagClass != null )
        { return false; }
        return !(country != null ? !country.equals( that.country ) : that.country != null);

    }

    @Override
    public int hashCode()
    {
        int result = tagClass != null ? tagClass.hashCode() : 0;
        result = 31 * result + (country != null ? country.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery4PopularCountryTopicsResult> marshalResult( String serializedResults )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery4PopularCountryTopicsResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long forumId = ((Number) row.get( 0 )).longValue();
            String title = (String) row.get( 1 );
            long creationDate = ((Number) row.get( 2 )).longValue();
            long moderator = ((Number) row.get( 3 )).longValue();
            int count = ((Number) row.get( 4 )).intValue();
            result.add(
                    new LdbcSnbBiQuery4PopularCountryTopicsResult(
                            forumId,
                            title,
                            creationDate,
                            moderator,
                            count
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery4PopularCountryTopicsResult> result =
                (List<LdbcSnbBiQuery4PopularCountryTopicsResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery4PopularCountryTopicsResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.forumId() );
            resultFields.add( row.forumTitle() );
            resultFields.add( row.forumCreationDate() );
            resultFields.add( row.moderatorId() );
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
