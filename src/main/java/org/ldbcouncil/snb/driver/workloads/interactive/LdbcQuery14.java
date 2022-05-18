

package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.base.Function;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Equator;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LdbcQuery14 extends Operation<List<LdbcQuery14Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 14;
    public static final String PERSON1_ID = "person1IdStartNode";
    public static final String PERSON2_ID = "person2IdEndNode";

    private final long person1IdStartNode;
    private final long person2IdEndNode;

    public LdbcQuery14(
        @JsonProperty("person1IdStartNode") long person1IdStartNode,
        @JsonProperty("person2IdEndNode") long person2IdEndNode
    )
    {
        this.person1IdStartNode = person1IdStartNode;
        this.person2IdEndNode = person2IdEndNode;
    }

    public long getPerson1IdStartNode()
    {
        return person1IdStartNode;
    }

    public long getPerson2IdEndNode()
    {
        return person2IdEndNode;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON1_ID, person1IdStartNode)
                .put(PERSON2_ID, person2IdEndNode)
                .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcQuery14 that = (LdbcQuery14) o;

        if ( person1IdStartNode != that.person1IdStartNode )
        { return false; }
        if ( person2IdEndNode != that.person2IdEndNode )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (person1IdStartNode ^ (person1IdStartNode >>> 32));
        result = 31 * result + (int) (person2IdEndNode ^ (person2IdEndNode >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery14{" +
               "person1IdStartNode=" + person1IdStartNode +
               ", person2IdEndNode=" + person2IdEndNode +
               '}';
    }

    @Override
    public List<LdbcQuery14Result> deserializeResult( String serializedResults ) throws IOException
    {
        List<LdbcQuery14Result> marshaledOperationResult;
        marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery14Result[].class));
        return marshaledOperationResult;
    }

    @Override
    public int type()
    {
        return TYPE;
    }


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
