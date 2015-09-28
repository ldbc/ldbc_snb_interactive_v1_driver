package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery17FriendshipTrianglesResult
{
    private final int count;

    public LdbcSnbBiQuery17FriendshipTrianglesResult( int count )
    {
        this.count = count;
    }

    public int count()
    {
        return count;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery17Result{" +
               "count=" + count +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery17FriendshipTrianglesResult that = (LdbcSnbBiQuery17FriendshipTrianglesResult) o;

        return count == that.count;

    }

    @Override
    public int hashCode()
    {
        return count;
    }
}
