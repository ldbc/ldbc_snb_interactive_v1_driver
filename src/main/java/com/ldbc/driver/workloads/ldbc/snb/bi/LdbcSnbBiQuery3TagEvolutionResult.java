package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery3TagEvolutionResult
{
    private final String tag;
    private final int countA;
    private final int countB;
    private final int difference;

    public LdbcSnbBiQuery3TagEvolutionResult(
            String tag,
            int countA,
            int countB,
            int difference )
    {
        this.tag = tag;
        this.countA = countA;
        this.countB = countB;
        this.difference = difference;
    }

    public String tag()
    {
        return tag;
    }

    public int countA()
    {
        return countA;
    }

    public int countB()
    {
        return countB;
    }

    public int difference()
    {
        return difference;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery3Result{" +
               "tag='" + tag + '\'' +
               ", countA=" + countA +
               ", countB=" + countB +
               ", difference=" + difference +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery3TagEvolutionResult that = (LdbcSnbBiQuery3TagEvolutionResult) o;

        if ( countA != that.countA )
        { return false; }
        if ( countB != that.countB )
        { return false; }
        if ( difference != that.difference )
        { return false; }
        return !(tag != null ? !tag.equals( that.tag ) : that.tag != null);

    }

    @Override
    public int hashCode()
    {
        int result = tag != null ? tag.hashCode() : 0;
        result = 31 * result + countA;
        result = 31 * result + countB;
        result = 31 * result + difference;
        return result;
    }
}
