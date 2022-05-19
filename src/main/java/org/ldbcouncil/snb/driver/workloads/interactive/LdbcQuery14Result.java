package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Equator;
import org.ldbcouncil.snb.driver.validation.ValidationEquality;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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


    // Code block below was moved from LdbcSnbInteractiveWorkload.java
    // TODO: find usage of comparison method here, since it does not appear to break any test
    // after moving from here.
    private static final Equator<LdbcQuery14Result> LDBC_QUERY_14_RESULT_EQUATOR = new Equator<LdbcQuery14Result>()
    {
        @Override
        public boolean equate( LdbcQuery14Result result1, LdbcQuery14Result result2 )
        {
            return result1.equals( result2 );
        }

        @Override
        public int hash( LdbcQuery14Result result )
        {
            return 1;
        }
    };

    private boolean resultsEqual(Object result1, Object result2 )
    {
        // TODO can this logic not be moved to LdbcQuery14Result class and performed in equals() method?
        /*
        Group results by weight, because results with same weight can come in any order
            Convert
                [(weight, [ids...]), ...]
            To
                Map<weight, [(weight, [ids...])]>
            */
        List<LdbcQuery14Result> typedResults1 = (List<LdbcQuery14Result>) result1;
        Map<Double,List<LdbcQuery14Result>> results1ByWeight = new HashMap<>();
        for ( LdbcQuery14Result typedResult : typedResults1 )
        {
            List<LdbcQuery14Result> resultByWeight = results1ByWeight.get( typedResult.getPathWeight() );
            if ( null == resultByWeight )
            {
                resultByWeight = new ArrayList<>();
            }
            resultByWeight.add( typedResult );
            results1ByWeight.put( typedResult.getPathWeight(), resultByWeight );
        }

        List<LdbcQuery14Result> typedResults2 = (List<LdbcQuery14Result>) result2;
        Map<Double,List<LdbcQuery14Result>> results2ByWeight = new HashMap<>();
        for ( LdbcQuery14Result typedResult : typedResults2 )
        {
            List<LdbcQuery14Result> resultByWeight = results2ByWeight.get( typedResult.getPathWeight() );
            if ( null == resultByWeight )
            {
                resultByWeight = new ArrayList<>();
            }
            resultByWeight.add( typedResult );
            results2ByWeight.put( typedResult.getPathWeight(), resultByWeight );
        }

        /*
        Perform equality check
            - compare set of keys
            - convert list of lists to set of lists & compare contains all for set of lists for each key
            */
        // compare set of keys
        if ( false == results1ByWeight.keySet().equals( results2ByWeight.keySet() ) )
        {
            return false;
        }
        // convert list of lists to set of lists & compare contains all for set of lists for each key
        for ( Double weight : results1ByWeight.keySet() )
        {
            if ( results1ByWeight.get( weight ).size() != results2ByWeight.get( weight ).size() )
            {
                return false;
            }

            if ( false == CollectionUtils
                    .isEqualCollection( results1ByWeight.get( weight ), results2ByWeight.get( weight ),
                            LDBC_QUERY_14_RESULT_EQUATOR ) )
            {
                return false;
            }
        }

        return true;
    }

}
