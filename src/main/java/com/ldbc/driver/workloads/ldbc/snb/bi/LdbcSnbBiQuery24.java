package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.List;

public class LdbcSnbBiQuery24 extends Operation<List<LdbcSnbBiQuery24Result>>
{
    public static final int TYPE = 24;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final String tagClass;
    private final int limit;

    public LdbcSnbBiQuery24( String tagClass, int limit )
    {
        this.tagClass = tagClass;
        this.limit = limit;
    }

    public String tagClass()
    {
        return tagClass;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery24{" +
               "tagClass='" + tagClass + '\'' +
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

        LdbcSnbBiQuery24 that = (LdbcSnbBiQuery24) o;

        if ( limit != that.limit )
        { return false; }
        return !(tagClass != null ? !tagClass.equals( that.tagClass ) : that.tagClass != null);

    }

    @Override
    public int hashCode()
    {
        int result = tagClass != null ? tagClass.hashCode() : 0;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery24Result> marshalResult( String serializedResults ) throws
            SerializingMarshallingException
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
