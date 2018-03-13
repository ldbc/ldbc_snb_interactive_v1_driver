package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LdbcSnbBiQuery11UnrelatedReplies extends Operation<List<LdbcSnbBiQuery11UnrelatedRepliesResult>>
{
    public static final int TYPE = 11;
    public static final int DEFAULT_LIMIT = 100;
    public static final String BLACKLIST = "blacklist";
    public static final String COUNTRY = "country";
    public static final String LIMIT = "limit";

    private final String country;
    private final List<String> blacklist;
    private final int limit;

    public LdbcSnbBiQuery11UnrelatedReplies(String country, List<String> blacklist, int limit )
    {
        this.country = country;
        this.blacklist = blacklist;
        this.limit = limit;
    }

    public List<String> blacklist()
    {
        return blacklist;
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
                .put(BLACKLIST, blacklist)
                .put(COUNTRY, country)
                .put(LIMIT, limit)
                .build();
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery11UnrelatedReplies{" +
               "country='" + country + '\'' +
               ", blacklist=" + blacklist +
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

        LdbcSnbBiQuery11UnrelatedReplies that = (LdbcSnbBiQuery11UnrelatedReplies) o;

        if ( limit != that.limit )
        { return false; }
        if ( country != null ? !country.equals( that.country ) : that.country != null )
        { return false; }
        return !(blacklist != null ? !blacklist.equals( that.blacklist) : that.blacklist != null);

    }

    @Override
    public int hashCode()
    {
        int result = country != null ? country.hashCode() : 0;
        result = 31 * result + (blacklist != null ? blacklist.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery11UnrelatedRepliesResult> marshalResult( String serializedResults )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery11UnrelatedRepliesResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long personId = ((Number) row.get( 0 )).longValue();
            String tagName = (String) row.get( 1 );
            int likeCount = ((Number) row.get( 2 )).intValue();
            int replyCount = ((Number) row.get( 3 )).intValue();
            result.add(
                    new LdbcSnbBiQuery11UnrelatedRepliesResult(
                            personId,
                            tagName,
                            likeCount,
                            replyCount
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery11UnrelatedRepliesResult> result =
                (List<LdbcSnbBiQuery11UnrelatedRepliesResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery11UnrelatedRepliesResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.personId() );
            resultFields.add( row.tag() );
            resultFields.add( row.likeCount() );
            resultFields.add( row.replyCount() );
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
