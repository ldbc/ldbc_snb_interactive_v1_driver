package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery21ZombiesResult
{
    private final long personId;
    private final int zombieCount;
    private final int realCount;
    private final int score;

    public LdbcSnbBiQuery21ZombiesResult( long personId, int zombieCount, int realCount, int score )
    {
        this.personId = personId;
        this.zombieCount = zombieCount;
        this.realCount = realCount;
        this.score = score;
    }

    public long personId()
    {
        return personId;
    }

    public int zombieCount()
    {
        return zombieCount;
    }

    public int realCount()
    {
        return realCount;
    }

    public int score()
    {
        return score;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery21Result{" +
               "personId=" + personId +
               ", zombieCount=" + zombieCount +
               ", realCount=" + realCount +
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

        LdbcSnbBiQuery21ZombiesResult that = (LdbcSnbBiQuery21ZombiesResult) o;

        if ( personId != that.personId )
        { return false; }
        if ( zombieCount != that.zombieCount )
        { return false; }
        if ( realCount != that.realCount )
        { return false; }
        return score == that.score;

    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + zombieCount;
        result = 31 * result + realCount;
        result = 31 * result + score;
        return result;
    }
}
