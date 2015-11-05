package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery22InternationalDialogResult
{
    private final long personId1;
    private final long personId2;
    private final int score;

    public LdbcSnbBiQuery22InternationalDialogResult( long personId1, long personId2, int score )
    {
        this.personId1 = personId1;
        this.personId2 = personId2;
        this.score = score;
    }

    public long personId1()
    {
        return personId1;
    }

    public long personId2()
    {
        return personId2;
    }

    public int score()
    {
        return score;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery22InternationalDialogResult{" +
               "personId1=" + personId1 +
               ", personId2=" + personId2 +
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

        LdbcSnbBiQuery22InternationalDialogResult that = (LdbcSnbBiQuery22InternationalDialogResult) o;

        if ( personId1 != that.personId1 )
        { return false; }
        if ( personId2 != that.personId2 )
        { return false; }
        return score == that.score;

    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId1 ^ (personId1 >>> 32));
        result = 31 * result + (int) (personId2 ^ (personId2 >>> 32));
        result = 31 * result + score;
        return result;
    }
}
