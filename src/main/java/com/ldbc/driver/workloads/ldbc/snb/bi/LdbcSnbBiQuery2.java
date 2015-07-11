package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.List;

public class LdbcSnbBiQuery2 extends Operation<List<LdbcSnbBiQuery2Result>>
{
    public static final int TYPE = 2;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final long dateA;
    private final long dateB;
    private final int limit;

    public LdbcSnbBiQuery2( long dateA, long dateB, int limit )
    {
        this.dateA = dateA;
        this.dateB = dateB;
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

    public int limit()
    {
        return limit;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        LdbcSnbBiQuery2 that = (LdbcSnbBiQuery2) o;

        if ( dateA != that.dateA )
        {
            return false;
        }
        if ( dateB != that.dateB )
        {
            return false;
        }
        if ( limit != that.limit )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (dateA ^ (dateA >>> 32));
        result = 31 * result + (int) (dateB ^ (dateB >>> 32));
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery2Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
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
