package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.List;

public class LdbcSnbBiQuery3 extends Operation<List<LdbcSnbBiQuery3Result>>
{
    public static final int TYPE = 3;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final long dateA;
    private final long dateB;
    private final int limit;

    public LdbcSnbBiQuery3( long dateA, long dateB, int limit )
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
    public String toString()
    {
        return "LdbcSnbBiQuery3{" +
               "dateA=" + dateA +
               ", dateB=" + dateB +
               ", limit=" + limit +
               '}';
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

        LdbcSnbBiQuery3 that = (LdbcSnbBiQuery3) o;

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
    public List<LdbcSnbBiQuery3Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
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
