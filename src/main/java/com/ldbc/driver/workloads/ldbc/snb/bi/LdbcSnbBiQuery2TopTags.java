package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery2TopTags extends Operation<List<LdbcSnbBiQuery2TopTagsResult>>
{
    public static final int TYPE = 2;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final long dateA;
    private final long dateB;
    private final String countryA;
    private final String countryB;
    private final int limit;

    public LdbcSnbBiQuery2TopTags( long dateA, long dateB, String countryA, String countryB, int limit )
    {
        this.dateA = dateA;
        this.dateB = dateB;
        this.countryA = countryA;
        this.countryB = countryB;
        this.limit = limit;
    }

    public long dateA()
    {
        return dateA;
    }

    public long dateB()
    {
        return dateB;
    }

    public String countryA()
    {
        return countryA;
    }

    public String countryB()
    {
        return countryB;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery2TopTags{" +
               "dateA=" + dateA +
               ", dateB=" + dateB +
               ", countryA='" + countryA + '\'' +
               ", countryB='" + countryB + '\'' +
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

        LdbcSnbBiQuery2TopTags that = (LdbcSnbBiQuery2TopTags) o;

        if ( dateA != that.dateA )
        { return false; }
        if ( dateB != that.dateB )
        { return false; }
        if ( limit != that.limit )
        { return false; }
        if ( countryA != null ? !countryA.equals( that.countryA ) : that.countryA != null )
        { return false; }
        return !(countryB != null ? !countryB.equals( that.countryB ) : that.countryB != null);

    }

    @Override
    public int hashCode()
    {
        int result = (int) (dateA ^ (dateA >>> 32));
        result = 31 * result + (int) (dateB ^ (dateB >>> 32));
        result = 31 * result + (countryA != null ? countryA.hashCode() : 0);
        result = 31 * result + (countryB != null ? countryB.hashCode() : 0);
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
