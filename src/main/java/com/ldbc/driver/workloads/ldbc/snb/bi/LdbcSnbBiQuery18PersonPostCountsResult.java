package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery18PersonPostCountsResult
{
    private final int postCount;
    private final int count;

    public LdbcSnbBiQuery18PersonPostCountsResult( int postCount, int count )
    {
        this.postCount = postCount;
        this.count = count;
    }

    public int postCount()
    {
        return postCount;
    }

    public int count()
    {
        return count;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery18Result{" +
               "postCount=" + postCount +
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

        LdbcSnbBiQuery18PersonPostCountsResult that = (LdbcSnbBiQuery18PersonPostCountsResult) o;

        if ( postCount != that.postCount )
        { return false; }
        return count == that.count;

    }

    @Override
    public int hashCode()
    {
        int result = postCount;
        result = 31 * result + count;
        return result;
    }
}
