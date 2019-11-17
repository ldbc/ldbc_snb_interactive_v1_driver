package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery3TagEvolutionResult
{
    private final String tagName;
    private final int countMonth1;
    private final int countMonth2;
    private final int diff;

    public LdbcSnbBiQuery3TagEvolutionResult(
            String tagName,
            int countMonth1,
            int countMonth2,
            int diff)
    {
        this.tagName = tagName;
        this.countMonth1 = countMonth1;
        this.countMonth2 = countMonth2;
        this.diff = diff;
    }

    public String tagName()
    {
        return tagName;
    }

    public int countMonth1()
    {
        return countMonth1;
    }

    public int countMonth2()
    {
        return countMonth2;
    }

    public int diff()
    {
        return diff;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery3TagEvolutionResult{" +
               "tagName='" + tagName + '\'' +
               ", countMonth1=" + countMonth1 +
               ", countMonth2=" + countMonth2 +
               ", diff=" + diff +
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

        if ( countMonth1 != that.countMonth1)
        { return false; }
        if ( countMonth2 != that.countMonth2)
        { return false; }
        if ( diff != that.diff)
        { return false; }
        return !(tagName != null ? !tagName.equals( that.tagName) : that.tagName != null);

    }

    @Override
    public int hashCode()
    {
        int result = tagName != null ? tagName.hashCode() : 0;
        result = 31 * result + countMonth1;
        result = 31 * result + countMonth2;
        result = 31 * result + diff;
        return result;
    }
}
