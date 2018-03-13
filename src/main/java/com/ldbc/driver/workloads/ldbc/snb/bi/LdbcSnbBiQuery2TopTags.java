package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LdbcSnbBiQuery2TopTags extends Operation<List<LdbcSnbBiQuery2TopTagsResult>>
{
    public static final int TYPE = 2;
    public static final int DEFAULT_LIMIT = 100;
    public static final String START_DATE = "startDate";
    public static final String END_DATE = "endDate";
    public static final String COUNTRY1 = "country1";
    public static final String COUNTRY2 = "country2";
    public static final String LIMIT = "limit";

    private final long startDate;
    private final long endDate;
    private final String country1;
    private final String country2;
    private final int limit;

    public LdbcSnbBiQuery2TopTags(
            long startDate,
            long endDate,
            String country1,
            String country2,
            int limit )
    {
        this.startDate = startDate;
        this.endDate = endDate;
        this.country1 = country1;
        this.country2 = country2;
        this.limit = limit;
    }

    public long startDate()
    {
        return startDate;
    }

    public long endDate()
    {
        return endDate;
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
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(START_DATE, startDate)
                .put(END_DATE, endDate)
                .put(COUNTRY1, country1)
                .put(COUNTRY2, country2)
                .put(LIMIT, limit)
                .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery2TopTags that = (LdbcSnbBiQuery2TopTags) o;

        if ( startDate != that.startDate)
        { return false; }
        if ( endDate != that.endDate)
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
        int result = (int) (startDate ^ (startDate >>> 32));
        result = 31 * result + (int) (endDate ^ (endDate >>> 32));
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
            String countryName = (String) row.get( 0 );
            int messageMonth = ((Number) row.get( 1 )).intValue();
            String personGender = (String) row.get( 2 );
            int ageGroup = ((Number) row.get( 3 )).intValue();
            String tagName = (String) row.get( 4 );
            int messageCount = ((Number) row.get( 5 )).intValue();

            result.add(
                    new LdbcSnbBiQuery2TopTagsResult(
                            countryName,
                            messageMonth,
                            personGender,
                            ageGroup,
                            tagName,
                            messageCount
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
            resultFields.add( row.countryName() );
            resultFields.add( row.messageMonth() );
            resultFields.add( row.personGender() );
            resultFields.add( row.ageGroup() );
            resultFields.add( row.tagName() );
            resultFields.add( row.messageCount() );
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
