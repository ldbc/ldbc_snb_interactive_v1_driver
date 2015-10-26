package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery10TagPersonResult
{
    private final long personId;
    private final int score;
    private final int friendsScore;

    public LdbcSnbBiQuery10TagPersonResult( long personId, int score, int friendsScore )
    {
        this.personId = personId;
        this.score = score;
        this.friendsScore = friendsScore;
    }

    public long personId()
    {
        return personId;
    }

    public int score()
    {
        return score;
    }

    public int friendsScore()
    {
        return friendsScore;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery10TagPersonResult{" +
               "personId=" + personId +
               ", score=" + score +
               ", friendsScore=" + friendsScore +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery10TagPersonResult that = (LdbcSnbBiQuery10TagPersonResult) o;

        if ( personId != that.personId )
        { return false; }
        if ( score != that.score )
        { return false; }
        return friendsScore == that.friendsScore;

    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + score;
        result = 31 * result + friendsScore;
        return result;
    }
}
