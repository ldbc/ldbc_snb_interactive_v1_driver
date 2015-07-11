package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.List;

public class LdbcSnbBiQuery11 extends Operation<List<LdbcSnbBiQuery11Result>>
{
    public static final int TYPE = 11;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final String keyWord;
    private final String country;
    private final int limit;

    public LdbcSnbBiQuery11( String keyWord, String country, int limit )
    {
        this.keyWord = keyWord;
        this.country = country;
        this.limit = limit;
    }

    public String keyWord()
    {
        return keyWord;
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
        return "LdbcSnbBiQuery11{" +
               "keyWord='" + keyWord + '\'' +
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

        LdbcSnbBiQuery11 that = (LdbcSnbBiQuery11) o;

        if ( limit != that.limit )
        { return false; }
        if ( keyWord != null ? !keyWord.equals( that.keyWord ) : that.keyWord != null )
        { return false; }
        return !(country != null ? !country.equals( that.country ) : that.country != null);

    }

    @Override
    public int hashCode()
    {
        int result = keyWord != null ? keyWord.hashCode() : 0;
        result = 31 * result + (country != null ? country.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery11Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
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
