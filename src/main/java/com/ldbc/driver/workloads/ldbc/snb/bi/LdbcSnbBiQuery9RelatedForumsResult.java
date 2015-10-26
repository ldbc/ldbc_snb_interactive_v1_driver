package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery9RelatedForumsResult
{
    private final long forumId;
    private final int sumA;
    private final int sumB;

    public LdbcSnbBiQuery9RelatedForumsResult( long forumId, int sumA, int sumB )
    {
        this.forumId = forumId;
        this.sumA = sumA;
        this.sumB = sumB;
    }

    public long forumId()
    {
        return forumId;
    }

    public int sumA()
    {
        return sumA;
    }

    public int sumB()
    {
        return sumB;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery9RelatedForumsResult{" +
               "forumId=" + forumId +
               ", sumA=" + sumA +
               ", sumB=" + sumB +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery9RelatedForumsResult that = (LdbcSnbBiQuery9RelatedForumsResult) o;

        if ( forumId != that.forumId )
        { return false; }
        if ( sumA != that.sumA )
        { return false; }
        return sumB == that.sumB;

    }

    @Override
    public int hashCode()
    {
        int result = (int) (forumId ^ (forumId >>> 32));
        result = 31 * result + sumA;
        result = 31 * result + sumB;
        return result;
    }
}
