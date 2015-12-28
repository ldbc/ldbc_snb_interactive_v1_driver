package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery23HolidayDestinationsResult
{
    private final String country;
    private final int month;
    private final int count;

    public LdbcSnbBiQuery23HolidayDestinationsResult( String country, int month, int count )
    {
        this.country = country;
        this.month = month;
        this.count = count;
    }

    public String countryName()
    {
        return country;
    }

    public int month()
    {
        return month;
    }

    public int messageCount()
    {
        return count;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery23HolidayDestinationsResult{" +
               "country='" + country + '\'' +
               ", month=" + month +
               ", count=" + count +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery23HolidayDestinationsResult that = (LdbcSnbBiQuery23HolidayDestinationsResult) o;

        if ( month != that.month )
        { return false; }
        if ( count != that.count )
        { return false; }
        return country != null ? country.equals( that.country ) : that.country == null;

    }

    @Override
    public int hashCode()
    {
        int result = country != null ? country.hashCode() : 0;
        result = 31 * result + month;
        result = 31 * result + count;
        return result;
    }
}
