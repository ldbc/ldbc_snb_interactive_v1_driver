package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery9RelatedForums extends Operation<List<LdbcSnbBiQuery9RelatedForumsResult>>
{
    public static final int TYPE = 9;
    public static final int DEFAULT_LIMIT = 100;
    private final String tagClassA;
    private final String tagClassB;
    private final int threshold;
    private final int limit;

    public LdbcSnbBiQuery9RelatedForums( String tagClassA, String tagClassB, int threshold, int limit )
    {
        this.tagClassA = tagClassA;
        this.tagClassB = tagClassB;
        this.threshold = threshold;
        this.limit = limit;
    }

    public String tagClassA()
    {
        return tagClassA;
    }

    public String tagClassB()
    {
        return tagClassB;
    }

    public int threshold()
    {
        return threshold;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery9RelatedForums{" +
               "tagClassA='" + tagClassA + '\'' +
               ", tagClassB='" + tagClassB + '\'' +
               ", threshold=" + threshold +
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

        LdbcSnbBiQuery9RelatedForums that = (LdbcSnbBiQuery9RelatedForums) o;

        if ( threshold != that.threshold )
        { return false; }
        if ( limit != that.limit )
        { return false; }
        if ( tagClassA != null ? !tagClassA.equals( that.tagClassA ) : that.tagClassA != null )
        { return false; }
        return !(tagClassB != null ? !tagClassB.equals( that.tagClassB ) : that.tagClassB != null);
    }

    @Override
    public int hashCode()
    {
        int result = tagClassA != null ? tagClassA.hashCode() : 0;
        result = 31 * result + (tagClassB != null ? tagClassB.hashCode() : 0);
        result = 31 * result + threshold;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery9RelatedForumsResult> marshalResult( String serializedResults )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery9RelatedForumsResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long forumId = ((Number) row.get( 0 )).longValue();
            int sumA = ((Number) row.get( 1 )).intValue();
            int sumB = ((Number) row.get( 2 )).intValue();
            result.add(
                    new LdbcSnbBiQuery9RelatedForumsResult(
                            forumId,
                            sumA,
                            sumB
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery9RelatedForumsResult> result = (List<LdbcSnbBiQuery9RelatedForumsResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery9RelatedForumsResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.forumId() );
            resultFields.add( row.sumA() );
            resultFields.add( row.sumB() );
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
