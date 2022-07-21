package org.ldbcouncil.snb.driver.workloads.interactive.queries;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Iterator;

public class LdbcQuery14Result
{
    private final Iterable<? extends Number> personIdsInPath;
    private final long pathWeight;

    public LdbcQuery14Result(
        @JsonProperty("personIdsInPath") Iterable<? extends Number> personIdsInPath,
        @JsonProperty("pathWeight")      long pathWeight
    )
    {
        this.personIdsInPath = personIdsInPath;
        this.pathWeight = pathWeight;
    }

    public Iterable<? extends Number> getPersonIdsInPath()
    {
        return personIdsInPath;
    }

    public long getPathWeight()
    {
        return pathWeight;
    }

    /**
     * This method does not check for the path as during validation it requires
     * any path but with the same weight
     */
    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        LdbcQuery14Result that = (LdbcQuery14Result) o;
        if ( that.pathWeight != pathWeight )
        {
            return false;
        }
        if ( null == personIdsInPath || null == that.personIdsInPath )
        {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        int result = Long.valueOf(pathWeight).hashCode();
        Iterator i = personIdsInPath.iterator();
        while (i.hasNext()) {
            Object obj = i.next();
            result = 31 * result + (obj==null ? 0 : obj.hashCode());
        }
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery14Result{" +
               "personIdsInPath=" + personIdsInPath +
               ", pathWeight=" + pathWeight +
               '}';
    }
}
