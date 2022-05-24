package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.ldbcouncil.snb.driver.util.ListUtils;
import org.ldbcouncil.snb.driver.validation.ValidationEquality;

import java.util.Iterator;

public class LdbcQuery14Result
{
    private final Iterable<? extends Number> personIdsInPath;
    private final double pathWeight;

    public LdbcQuery14Result(
        @JsonProperty("personIdsInPath") Iterable<? extends Number> personIdsInPath,
        @JsonProperty("pathWeight")      double pathWeight
    )
    {
        this.personIdsInPath = personIdsInPath;
        this.pathWeight = pathWeight;
    }

    public Iterable<? extends Number> getPersonIdsInPath()
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

    /**
     * Checks for Iterable<LdbcQuery14Result> if the pathweights and objects are equal.
     * Note that this is different from the equals() function which compares only 1 object.
     * @param resultA LdbcQuery14Result iterable expected
     * @param resultB LdbcQuery14Result iterable actual
     * @return True if equal, otherwise false
     */
    public static boolean resultListEqual( Object resultA, Object resultB )
    {
        Iterable<LdbcQuery14Result> iterableResultA = ( Iterable<LdbcQuery14Result> ) resultA;
        Iterable<LdbcQuery14Result> iterableResultB = ( Iterable<LdbcQuery14Result> ) resultB;

        if (!ListUtils.listsEqual(iterableResultA, iterableResultB))
            { return false; }

        Iterator<LdbcQuery14Result> resultAIterator = iterableResultA.iterator();
        Iterator<LdbcQuery14Result> resultBIterator = iterableResultB.iterator();

        while ( resultAIterator.hasNext() )
        {
            if ( !resultBIterator.hasNext() )
                { return false; }

            LdbcQuery14Result resultAQ14Result = resultAIterator.next();
            LdbcQuery14Result resultBQ14Result = resultBIterator.next();
            double pathWeightA = resultAQ14Result.getPathWeight();
            double pathWeightB = resultBQ14Result.getPathWeight();

            if ( !ValidationEquality.doubleEquals( pathWeightA, pathWeightB ))
            { return false; }
        }
        return true;
    }

    /**
     * Check if the list of paths is equal. Note that when two paths has the same weight, 
     * order is unspecified.
     * @param path1 
     * @param path2
     * @return
     */
    private boolean personIdPathsEqual( Iterable<? extends Number> path1, Iterable<? extends Number> path2 )
    {
        Iterator<? extends Number> path1Iterator = path1.iterator();
        Iterator<? extends Number> path2Iterator = path2.iterator();
        while ( path1Iterator.hasNext() )
        {
            if ( !path2Iterator.hasNext() )
            { return false; }
            Number path1IdNumber = path1Iterator.next();
            Number path2IdNumber = path2Iterator.next();
            if ( null == path1IdNumber || null == path2IdNumber )
            { return false; }
            long path1Id = path1IdNumber.longValue();
            long path2Id = path2IdNumber.longValue();
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
