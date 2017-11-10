package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery2TopTags extends Operation<List<LdbcSnbBiQuery2TopTagsResult>>
{
    public static final int TYPE = 2;
    public static final int DEFAULT_LIMIT = 100;
    private final long date1;
    private final long date2;
    private final String country1;
    private final String country2;
    private final int limit;

    public LdbcSnbBiQuery2TopTags(
            long date1,
            long date2,
            String country1,
            String country2,
            int limit )
    {
        this.date1 = date1;
        this.date2 = date2;
        this.country1 = country1;
        this.country2 = country2;
        this.limit = limit;
    }

    public long date1()
    {
        return date1;
    }

    public long date2()
    {
        return date2;
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
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery2TopTags that = (LdbcSnbBiQuery2TopTags) o;

        if ( date1 != that.date1 )
        { return false; }
        if ( date2 != that.date2 )
        { return false; }
        if ( limit != that.limit )
        { return false; }
        if ( country1 != null ? !country1.equals( that.country1 ) : that.country1 != null )
        { return false; }
        return country2 != null ? country2.equals( that.country2 ) : that.country2 == null;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (date1 ^ (date1 >>> 32));
        result = 31 * result + (int) (date2 ^ (date2 >>> 32));
        result = 31 * result + (country1 != null ? country1.hashCode() : 0);
        result = 31 * result + (country2 != null ? country2.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery2TopTagsResult> marshalResult( String serializedResults )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery2TopTagsResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            String country = (String) row.get( 0 );
            int month = ((Number) row.get( 1 )).intValue();
            String gender = (String) row.get( 2 );
            int ageGroup = ((Number) row.get( 3 )).intValue();
            String tag = (String) row.get( 4 );
            int count = ((Number) row.get( 5 )).intValue();

            result.add(
                    new LdbcSnbBiQuery2TopTagsResult(
                            country,
                            month,
                            gender,
                            ageGroup,
                            tag,
                            count
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery2TopTagsResult> result = (List<LdbcSnbBiQuery2TopTagsResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery2TopTagsResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.country() );
            resultFields.add( row.month() );
            resultFields.add( row.gender() );
            resultFields.add( row.ageGroup() );
            resultFields.add( row.tag() );
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
