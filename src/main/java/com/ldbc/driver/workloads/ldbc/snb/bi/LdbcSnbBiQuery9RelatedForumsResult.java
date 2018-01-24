package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery9RelatedForumsResult
{
    private final long forumId;
    private final int count1;
    private final int count2;

    public LdbcSnbBiQuery9RelatedForumsResult(long forumId, int count1, int count2)
    {
        this.forumId = forumId;
        this.count1 = count1;
        this.count2 = count2;
    }

    public long forumId()
    {
        return forumId;
    }

    public int count1()
    {
        return count1;
    }

    public int count2()
    {
        return count2;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery9RelatedForumsResult{" +
               "forumId=" + forumId +
               ", count1=" + count1 +
               ", count2=" + count2 +
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
        if ( count1 != that.count1)
        { return false; }
        return count2 == that.count2;

    }

    @Override
    public int hashCode()
    {
        int result = (int) (forumId ^ (forumId >>> 32));
        result = 31 * result + count1;
        result = 31 * result + count2;
        return result;
    }
}
