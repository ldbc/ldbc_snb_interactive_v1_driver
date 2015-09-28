package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery11UnrelatedReplies extends Operation<List<LdbcSnbBiQuery11UnrelatedRepliesResult>>
{
    public static final int TYPE = 11;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final String keyWord;
    private final String country;
    private final int limit;

    public LdbcSnbBiQuery11UnrelatedReplies( String keyWord, String country, int limit )
    {
        this.keyWord = keyWord;
        this.country = country;
        this.limit = limit;
    }

    public String keyWord()
    {
        return keyWord;
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
        return "LdbcSnbBiQuery11{" +
               "keyWord='" + keyWord + '\'' +
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

        LdbcSnbBiQuery11UnrelatedReplies that = (LdbcSnbBiQuery11UnrelatedReplies) o;

        if ( limit != that.limit )
        { return false; }
        if ( keyWord != null ? !keyWord.equals( that.keyWord ) : that.keyWord != null )
        { return false; }
        return !(country != null ? !country.equals( that.country ) : that.country != null);

    }

    @Override
    public int hashCode()
    {
        int result = keyWord != null ? keyWord.hashCode() : 0;
        result = 31 * result + (country != null ? country.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery11UnrelatedRepliesResult> marshalResult( String serializedResults ) throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery11UnrelatedRepliesResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long personId = ((Number) row.get( 0 )).longValue();
            String tag = (String) row.get( 1 );
            int likeCount = ((Number) row.get( 2 )).intValue();
            int replyCount = ((Number) row.get( 3 )).intValue();
            result.add(
                    new LdbcSnbBiQuery11UnrelatedRepliesResult(
                            personId,
                            tag,
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
        List<LdbcSnbBiQuery11UnrelatedRepliesResult> result = (List<LdbcSnbBiQuery11UnrelatedRepliesResult>) resultsObject;
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
