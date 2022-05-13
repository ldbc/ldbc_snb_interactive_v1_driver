package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.ldbcouncil.snb.driver.validation.ValidationEquality;

import java.util.Iterator;

public class LdbcQuery14Result
{
    private final Iterable<Long> personIdsInPath;
    private final double pathWeight;

    public LdbcQuery14Result(
        @JsonProperty("personIdsInPath") Iterable<Long> personIdsInPath,
        @JsonProperty("pathWeight") double pathWeight )
    {
        this.personIdsInPath = personIdsInPath;
        this.pathWeight = pathWeight;
    }

    public Iterable<Long> getPersonsIdsInPath()
    {
        return personIdsInPath;
    }

    public double getPathWeight()
    {
        return pathWeight;
    }

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
        if ( !ValidationEquality.doubleEquals( that.pathWeight, pathWeight ) )
        {
            return false;
        }
        if ( null == personIdsInPath || null == that.personIdsInPath )
        {
            return false;
        }
        return personIdPathsEqual( personIdsInPath, that.personIdsInPath );
    }

    private boolean personIdPathsEqual( Iterable<Long> path1, Iterable<Long> path2 )
    {
        Iterator<? extends Long> path1Iterator = path1.iterator();
        Iterator<? extends Long> path2Iterator = path2.iterator();
        while ( path1Iterator.hasNext() )
        {
            if ( !path2Iterator.hasNext() )
            { return false; }
            Long path1IdLong = path1Iterator.next();
            Long path2IdLong = path2Iterator.next();
            if ( null == path1IdLong || null == path2IdLong )
            { return false; }
            long path1Id = path1IdLong.longValue();
            long path2Id = path2IdLong.longValue();
            if ( path1Id != path2Id )
            { return false; }
        }
        return !path2Iterator.hasNext();
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