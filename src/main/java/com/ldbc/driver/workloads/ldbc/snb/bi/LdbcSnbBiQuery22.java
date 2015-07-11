package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.List;

public class LdbcSnbBiQuery22 extends Operation<List<LdbcSnbBiQuery22Result>>
{
    public static final int TYPE = 22;
    // TODO
    public static final int DEFAULT_LIMIT = 20;
    private final String countryA;
    private final String countryB;
    private final int limit;

    public LdbcSnbBiQuery22( String countryA, String countryB, int limit )
    {
        this.countryA = countryA;
        this.countryB = countryB;
        this.limit = limit;
    }

    public String countryA()
    {
        return countryA;
    }

    public String countryB()
    {
        return countryB;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery22{" +
               "countryA='" + countryA + '\'' +
               ", countryB='" + countryB + '\'' +
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

        LdbcSnbBiQuery22 that = (LdbcSnbBiQuery22) o;

        if ( limit != that.limit )
        { return false; }
        if ( countryA != null ? !countryA.equals( that.countryA ) : that.countryA != null )
        { return false; }
        return !(countryB != null ? !countryB.equals( that.countryB ) : that.countryB != null);

    }

    @Override
    public int hashCode()
    {
        int result = countryA != null ? countryA.hashCode() : 0;
        result = 31 * result + (countryB != null ? countryB.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery22Result> marshalResult( String serializedResults ) throws
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
