package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.validation.ValidationEquality;

public class LdbcSnbBiQuery21ZombiesResult
{
    private final long personId;
    private final int zombieCount;
    private final int realCount;
    private final double score;

    public LdbcSnbBiQuery21ZombiesResult( long personId, int zombieCount, int realCount, double score )
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

    public double score()
    {
        return score;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery21ZombiesResult{" +
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
        return ValidationEquality.doubleEquals( that.score, score );

    }

    @Override
    public int hashCode()
    {
        int result;
        long temp;
        result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + zombieCount;
        result = 31 * result + realCount;
        temp = Double.doubleToLongBits( score );
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
