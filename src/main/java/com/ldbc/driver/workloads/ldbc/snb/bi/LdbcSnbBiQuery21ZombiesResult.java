package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.validation.ValidationEquality;

public class LdbcSnbBiQuery21ZombiesResult
{
    private final long personId;
    private final int zombieLikeCount;
    private final int totalLikeCount;
    private final double zombieScore;

    public LdbcSnbBiQuery21ZombiesResult(long personId, int zombieLikeCount, int totalLikeCount, double zombieScore)
    {
        this.personId = personId;
        this.zombieLikeCount = zombieLikeCount;
        this.totalLikeCount = totalLikeCount;
        this.zombieScore = zombieScore;
    }

    public long personId()
    {
        return personId;
    }

    public int zombieLikeCount()
    {
        return zombieLikeCount;
    }

    public int totalLikeCount()
    {
        return totalLikeCount;
    }

    public double zombieScore()
    {
        return zombieScore;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery21ZombiesResult{" +
               "personId=" + personId +
               ", zombieLikeCount=" + zombieLikeCount +
               ", totalLikeCount=" + totalLikeCount +
               ", zombieScore=" + zombieScore +
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
        if ( zombieLikeCount != that.zombieLikeCount)
        { return false; }
        if ( totalLikeCount != that.totalLikeCount)
        { return false; }
        return ValidationEquality.doubleEquals( that.zombieScore, zombieScore);

    }

    @Override
    public int hashCode()
    {
        int result;
        long temp;
        result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + zombieLikeCount;
        result = 31 * result + totalLikeCount;
        temp = Double.doubleToLongBits(zombieScore);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
