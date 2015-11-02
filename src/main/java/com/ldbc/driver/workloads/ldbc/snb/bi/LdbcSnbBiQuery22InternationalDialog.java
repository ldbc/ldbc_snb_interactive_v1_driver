package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery22InternationalDialog extends Operation<List<LdbcSnbBiQuery22InternationalDialogResult>>
{
    public static final int TYPE = 22;
    public static final int DEFAULT_LIMIT = 100;
    private final String countryA;
    private final String countryB;
    private final int limit;

    public LdbcSnbBiQuery22InternationalDialog( String countryA, String countryB, int limit )
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

        LdbcSnbBiQuery22InternationalDialog that = (LdbcSnbBiQuery22InternationalDialog) o;

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
    public List<LdbcSnbBiQuery22InternationalDialogResult> marshalResult( String serializedResults ) throws
            SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery22InternationalDialogResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            // TODO update when result is defined
            result.add(
                    new LdbcSnbBiQuery22InternationalDialogResult()
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery23HolidayDestinationsResult> result =
                (List<LdbcSnbBiQuery23HolidayDestinationsResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            // TODO update when result is defined
            List<Object> resultFields = new ArrayList<>();
            resultsFields.add( resultFields );
        }
        return SerializationUtil.toJson( resultsFields );
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
