package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery15SocialNormalsResult
{
    private final long personId;
    private final int count;

    public LdbcSnbBiQuery15SocialNormalsResult( long personId, int count )
    {
        this.personId = personId;
        this.count = count;
    }

    public long personId()
    {
        return personId;
    }

    public int count()
    {
        return count;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery15SocialNormalsResult{" +
               "personId=" + personId +
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

        LdbcSnbBiQuery15SocialNormalsResult that = (LdbcSnbBiQuery15SocialNormalsResult) o;

        if ( personId != that.personId )
        { return false; }
        return count == that.count;

    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + count;
        return result;
    }
}
