package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LdbcSnbBiQuery18PersonPostCounts extends Operation<List<LdbcSnbBiQuery18PersonPostCountsResult>>
{
    public static final int TYPE = 18;
    public static final int DEFAULT_LIMIT = 100;
    public static final String DATE = "date";
    public static final String LENGTH_THRESHOLD = "lengthThreshold";
    public static final String LANGUAGES = "languages";
    public static final String LIMIT = "limit";

    private final long date;
    private final int lengthThreshold;
    private final List<String> languages;
    private final int limit;

    public LdbcSnbBiQuery18PersonPostCounts( long date, int lengthThreshold, List<String> languages, int limit )
    {
        this.date = date;
        this.lengthThreshold = lengthThreshold;
        this.languages = languages;
        this.limit = limit;
    }

    public long date()
    {
        return date;
    }

    public int lengthThreshold()
    {
        return lengthThreshold;
    }

    public List<String> languages()
    {
        return languages;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(DATE, date)
                .put(LENGTH_THRESHOLD, lengthThreshold)
                .put(LANGUAGES, languages)
                .put(LIMIT, limit)
                .build();
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery18PersonPostCounts{" +
               "date=" + date +
               "lengthThreshold=" + lengthThreshold +
               "languages=" + languages +
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

        LdbcSnbBiQuery18PersonPostCounts that = (LdbcSnbBiQuery18PersonPostCounts) o;

        if ( date != that.date )
        { return false; }
        if ( lengthThreshold != that.lengthThreshold )
        { return false; }
        if ( limit != that.limit )
        { return false; }
        return languages != null ? languages.equals( that.languages ) : that.languages == null;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (date ^ (date >>> 32));
        result = 31 * result + lengthThreshold;
        result = 31 * result + (languages != null ? languages.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery18PersonPostCountsResult> marshalResult( String serializedResults )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery18PersonPostCountsResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            int postCount = ((Number) row.get( 0 )).intValue();
            int count = ((Number) row.get( 1 )).intValue();
            result.add(
                    new LdbcSnbBiQuery18PersonPostCountsResult(
                            postCount,
                            count
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery18PersonPostCountsResult> result =
                (List<LdbcSnbBiQuery18PersonPostCountsResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery18PersonPostCountsResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.messageCount() );
            resultFields.add( row.personCount() );
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
