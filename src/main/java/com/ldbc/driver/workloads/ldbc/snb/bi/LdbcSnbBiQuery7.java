package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.List;

public class LdbcSnbBiQuery7 extends Operation<List<LdbcSnbBiQuery7Result>>
{
    public static final int TYPE = 7;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final String tag;
    private final int limit;

    public LdbcSnbBiQuery7( String tag, int limit )
    {
        this.tag = tag;
        this.limit = limit;
    }

    public String tag()
    {
        return tag;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery7{" +
               "tag='" + tag + '\'' +
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

        LdbcSnbBiQuery7 that = (LdbcSnbBiQuery7) o;

        if ( limit != that.limit )
        { return false; }
        return !(tag != null ? !tag.equals( that.tag ) : that.tag != null);

    }

    @Override
    public int hashCode()
    {
        int result = tag != null ? tag.hashCode() : 0;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery7Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
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
