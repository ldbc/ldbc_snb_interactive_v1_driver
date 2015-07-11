package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.List;

public class LdbcSnbBiQuery16 extends Operation<List<LdbcSnbBiQuery16Result>>
{
    public static final int TYPE = 16;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final String tagClass;
    private final String country;
    private final int limit;

    public LdbcSnbBiQuery16( String tagClass, String country, int limit )
    {
        this.tagClass = tagClass;
        this.country = country;
        this.limit = limit;
    }

    public String tagClass()
    {
        return tagClass;
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
        return "LdbcSnbBiQuery16{" +
               "tagClass='" + tagClass + '\'' +
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

        LdbcSnbBiQuery16 that = (LdbcSnbBiQuery16) o;

        if ( limit != that.limit )
        { return false; }
        if ( tagClass != null ? !tagClass.equals( that.tagClass ) : that.tagClass != null )
        { return false; }
        return !(country != null ? !country.equals( that.country ) : that.country != null);

    }

    @Override
    public int hashCode()
    {
        int result = tagClass != null ? tagClass.hashCode() : 0;
        result = 31 * result + (country != null ? country.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery16Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
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
