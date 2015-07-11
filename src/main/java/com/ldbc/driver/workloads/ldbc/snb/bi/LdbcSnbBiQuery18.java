package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.List;

public class LdbcSnbBiQuery18 extends Operation<List<LdbcSnbBiQuery18Result>>
{
    public static final int TYPE = 18;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final long date;
    private final int limit;

    public LdbcSnbBiQuery18( long date, int limit )
    {
        this.date = date;
        this.limit = limit;
    }

    public long date()
    {
        return date;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery18{" +
               "date=" + date +
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

        LdbcSnbBiQuery18 that = (LdbcSnbBiQuery18) o;

        if ( date != that.date )
        { return false; }
        return limit == that.limit;

    }

    @Override
    public int hashCode()
    {
        int result = (int) (date ^ (date >>> 32));
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery18Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
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
