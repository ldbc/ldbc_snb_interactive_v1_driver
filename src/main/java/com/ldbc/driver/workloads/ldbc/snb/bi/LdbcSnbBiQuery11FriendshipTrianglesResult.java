package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery11FriendshipTrianglesResult
{
    private final int count;

    public LdbcSnbBiQuery11FriendshipTrianglesResult( int count )
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
        return "LdbcSnbBiQuery11FriendshipTrianglesResult{" +
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

        LdbcSnbBiQuery11FriendshipTrianglesResult that = (LdbcSnbBiQuery11FriendshipTrianglesResult) o;

        return count == that.count;

    }

    @Override
    public int hashCode()
    {
        return count;
    }
}
