package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery23HolidayDestinations extends Operation<List<LdbcSnbBiQuery23HolidayDestinationsResult>>
{
    public static final int TYPE = 23;
    public static final int DEFAULT_LIMIT = 100;
    private final String country;
    private final int limit;

    public LdbcSnbBiQuery23HolidayDestinations( String country, int limit )
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
        return "LdbcSnbBiQuery23{" +
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

        LdbcSnbBiQuery23HolidayDestinations that = (LdbcSnbBiQuery23HolidayDestinations) o;

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
    public List<LdbcSnbBiQuery23HolidayDestinationsResult> marshalResult( String serializedResults ) throws
            SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery23HolidayDestinationsResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            String place = (String) row.get( 0 );
            int month = ((Number) row.get( 1 )).intValue();
            int count = ((Number) row.get( 2 )).intValue();
            result.add(
                    new LdbcSnbBiQuery23HolidayDestinationsResult(
                            place,
                            month,
                            count
                    )
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
            LdbcSnbBiQuery23HolidayDestinationsResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.countryName() );
            resultFields.add( row.month() );
            resultFields.add( row.messageCount() );
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
