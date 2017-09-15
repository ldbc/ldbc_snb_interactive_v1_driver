package com.ldbc.driver.workloads.ldbc.snb.bi;

import java.util.List;

public class LdbcSnbBiQuery25WeightedPathsResult
{
    private final List<Long> personIds;

    public LdbcSnbBiQuery25WeightedPathsResult( List<Long> personIds )
    {
        this.personIds = personIds;
    }

    public List<Long> personIds()
    {
        return personIds;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery25WeightedPathsResult{" +
               "personIds=" + personIds +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery25WeightedPathsResult that = (LdbcSnbBiQuery25WeightedPathsResult) o;

        return !(personIds != null ? !personIds.equals( that.personIds )
                                   : that.personIds() != null);
    }

    @Override
    public int hashCode()
    {
        int result = personIds().hashCode();
        return result;
    }
}
