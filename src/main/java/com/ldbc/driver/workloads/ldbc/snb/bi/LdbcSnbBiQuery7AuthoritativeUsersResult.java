package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery7AuthoritativeUsersResult
{
    private final long personId;
    private final int score;

    public LdbcSnbBiQuery7AuthoritativeUsersResult( long personId, int score )
    {
        this.personId = personId;
        this.score = score;
    }

    public long personId()
    {
        return personId;
    }

    public int score()
    {
        return score;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery7Result{" +
               "personId=" + personId +
               ", score=" + score +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery7AuthoritativeUsersResult that = (LdbcSnbBiQuery7AuthoritativeUsersResult) o;

        if ( personId != that.personId )
        { return false; }
        return score == that.score;

    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + score;
        return result;
    }
}
