package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.List;

public class LdbcSnbBiQuery20 extends Operation<List<LdbcSnbBiQuery20Result>>
{
    public static final int TYPE = 20;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final int limit;

    public LdbcSnbBiQuery20( int limit )
    {
        this.limit = limit;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery20{" +
               "limit=" + limit +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery20 that = (LdbcSnbBiQuery20) o;

        return limit == that.limit;

    }

    @Override
    public int hashCode()
    {
        return limit;
    }

    @Override
    public List<LdbcSnbBiQuery20Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
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
