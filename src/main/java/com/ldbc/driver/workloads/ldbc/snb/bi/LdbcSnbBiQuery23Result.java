package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery23Result
{
    private final String place;
    private final int month;
    private final int count;

    public LdbcSnbBiQuery23Result( String place, int month, int count )
    {
        this.place = place;
        this.month = month;
        this.count = count;
    }

    public String place()
    {
        return place;
    }

    public int month()
    {
        return month;
    }

    public int count()
    {
        return count;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery23Result{" +
               "place='" + place + '\'' +
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

        LdbcSnbBiQuery23Result that = (LdbcSnbBiQuery23Result) o;

        if ( month != that.month )
        { return false; }
        if ( count != that.count )
        { return false; }
        return !(place != null ? !place.equals( that.place ) : that.place != null);

    }

    @Override
    public int hashCode()
    {
        int result = place != null ? place.hashCode() : 0;
        result = 31 * result + month;
        result = 31 * result + count;
        return result;
    }
}
