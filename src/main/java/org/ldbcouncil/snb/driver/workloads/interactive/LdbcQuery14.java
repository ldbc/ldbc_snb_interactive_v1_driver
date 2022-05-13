

package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.base.Function;

import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
        // List<List<Object>> resultsAsList;
        // resultsAsList = OBJECT_MAPPER.readValue(
        //         serializedResults,
        //         new TypeReference<List<List<Object>>>()
        //         {
        //         }
        // );

        // List<LdbcQuery14Result> results = new ArrayList<>();
        // for ( int i = 0; i < resultsAsList.size(); i++ )
        // {
        //     List<Object> resultAsList = resultsAsList.get( i );
        //     Iterable<Long> personsIdsInPath =
        //             Iterables.transform( (List<Number>) resultAsList.get( 0 ), new Function<Number,Long>()
        //             {
        //                 @Override
        //                 public Long apply( Number number )
        //                 {
        //                     return number.longValue();
        //                 }
        //             } );
        //     double pathWeight = ((Number) resultAsList.get( 1 )).doubleValue();

        //     results.add(
        //             new LdbcQuery14Result(
        //                     personsIdsInPath,
        //                     pathWeight
        //             )
        //     );
        // }
        // return results;
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
