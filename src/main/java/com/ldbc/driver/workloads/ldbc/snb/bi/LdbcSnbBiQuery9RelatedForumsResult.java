package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery9RelatedForumsResult
{
    private final String forumTitle;
    private final int sumA;
    private final int sumB;

    public LdbcSnbBiQuery9RelatedForumsResult( String forumTitle, int sumA, int sumB )
    {
        this.forumTitle = forumTitle;
        this.sumA = sumA;
        this.sumB = sumB;
    }

    public String forumTitle()
    {
        return forumTitle;
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
        return "LdbcSnbBiQuery9Result{" +
               "forumTitle='" + forumTitle + '\'' +
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

        if ( sumA != that.sumA )
        { return false; }
        if ( sumB != that.sumB )
        { return false; }
        return !(forumTitle != null ? !forumTitle.equals( that.forumTitle ) : that.forumTitle != null);

    }

    @Override
    public int hashCode()
    {
        int result = forumTitle != null ? forumTitle.hashCode() : 0;
        result = 31 * result + sumA;
        result = 31 * result + sumB;
        return result;
    }
}
