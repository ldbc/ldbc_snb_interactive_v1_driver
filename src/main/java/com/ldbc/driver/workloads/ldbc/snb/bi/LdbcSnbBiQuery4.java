package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.List;

public class LdbcSnbBiQuery4 extends Operation<List<LdbcSnbBiQuery4Result>>
{
    public static final int TYPE = 4;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final String country;
    private final int limit;

    public LdbcSnbBiQuery4( String country, int limit )
    {
        this.country = country;
        this.limit = limit;
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
        return "LdbcSnbBiQuery4{" +
               "country='" + country + '\'' +
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

        LdbcSnbBiQuery4 that = (LdbcSnbBiQuery4) o;

        if ( limit != that.limit )
        { return false; }
        return !(country != null ? !country.equals( that.country ) : that.country != null);

    }

    @Override
    public int hashCode()
    {
        int result = country != null ? country.hashCode() : 0;
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery4Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
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
