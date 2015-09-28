package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery13PopularMonthlyTagsResult
{
    private final int year;
    private final int month;
    private final String tag;
    private final int count;

    public LdbcSnbBiQuery13PopularMonthlyTagsResult( int year, int month, String tag, int count )
    {
        this.year = year;
        this.month = month;
        this.tag = tag;
        this.count = count;
    }

    public int year()
    {
        return year;
    }

    public int month()
    {
        return month;
    }

    public String tag()
    {
        return tag;
    }

    public int count()
    {
        return count;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery13Result{" +
               "year=" + year +
               ", month=" + month +
               ", tag='" + tag + '\'' +
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

        LdbcSnbBiQuery13PopularMonthlyTagsResult that = (LdbcSnbBiQuery13PopularMonthlyTagsResult) o;

        if ( year != that.year )
        { return false; }
        if ( month != that.month )
        { return false; }
        if ( count != that.count )
        { return false; }
        return !(tag != null ? !tag.equals( that.tag ) : that.tag != null);

    }

    @Override
    public int hashCode()
    {
        int result = year;
        result = 31 * result + month;
        result = 31 * result + (tag != null ? tag.hashCode() : 0);
        result = 31 * result + count;
        return result;
    }
}
