package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.List;

public class LdbcSnbBiQuery10 extends Operation<List<LdbcSnbBiQuery10Result>>
{
    public static final int TYPE = 10;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final String tagA;
    private final String tagB;
    private final int limit;

    public LdbcSnbBiQuery10( String tagA, String tagB, int limit )
    {
        this.tagA = tagA;
        this.tagB = tagB;
        this.limit = limit;
    }

    public String tagA()
    {
        return tagA;
    }

    public String tagB()
    {
        return tagB;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery10{" +
               "tagA='" + tagA + '\'' +
               ", tagB='" + tagB + '\'' +
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

        LdbcSnbBiQuery10 that = (LdbcSnbBiQuery10) o;

        if ( limit != that.limit )
        { return false; }
        if ( tagA != null ? !tagA.equals( that.tagA ) : that.tagA != null )
        { return false; }
        return !(tagB != null ? !tagB.equals( that.tagB ) : that.tagB != null);

    }

    @Override
    public int hashCode()
    {
        int result = tagA != null ? tagA.hashCode() : 0;
        result = 31 * result + (tagB != null ? tagB.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery10Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
