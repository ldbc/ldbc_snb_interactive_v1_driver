package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery24MessagesByTopicResult
{
    private final int year;
    private final int month;
    private final String continent;
    private final int postCount;

    public LdbcSnbBiQuery24MessagesByTopicResult( int year, int month, String continent, int postCount )
    {
        this.year = year;
        this.month = month;
        this.continent = continent;
        this.postCount = postCount;
    }

    public int year()
    {
        return year;
    }

    public int month()
    {
        return month;
    }

    public String continent()
    {
        return continent;
    }

    public int postCount()
    {
        return postCount;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery24Result{" +
               "year=" + year +
               ", month=" + month +
               ", continent='" + continent + '\'' +
               ", postCount=" + postCount +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery24MessagesByTopicResult that = (LdbcSnbBiQuery24MessagesByTopicResult) o;

        if ( year != that.year )
        { return false; }
        if ( month != that.month )
        { return false; }
        if ( postCount != that.postCount )
        { return false; }
        return !(continent != null ? !continent.equals( that.continent ) : that.continent != null);

    }

    @Override
    public int hashCode()
    {
        int result = year;
        result = 31 * result + month;
        result = 31 * result + (continent != null ? continent.hashCode() : 0);
        result = 31 * result + postCount;
        return result;
    }
}
