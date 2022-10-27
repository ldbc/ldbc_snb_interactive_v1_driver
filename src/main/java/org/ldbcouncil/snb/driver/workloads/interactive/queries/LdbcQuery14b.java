package org.ldbcouncil.snb.driver.workloads.interactive.queries;
/**
 * LdbcQuery14.java
 * 
 * Interactive workload complex read query 14:
 * -- Trusted connection paths--
 * 
 * Find a cheapest path between the given Persons, in the interaction subgraph.
 * (If there are multiple cheapest paths, return any of them can be returned).
 * The interaction subgraph is based on the Person-knows-Person graph where the
 * Person endpoints of each knows edge have at least one interaction between them.
 * An interaction is defined as a direct reply Comment (by one of the Persons)
 * to a Message (by the other Person). The edge weights are determined as
 * max(floor(40 - sqrt(numInteractions)), 1).
 */


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import org.ldbcouncil.snb.driver.workloads.interactive.LdbcOperation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LdbcQuery14b extends LdbcOperation<List<LdbcQuery14Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 142;
    // Parameters used for replacement in queries
    public static final String PERSON1_ID = "person1Id";
    public static final String PERSON2_ID = "person2Id";

    private final long person1IdQ14StartNode;
    private final long person2IdQ14EndNode;

    public LdbcQuery14b(
        @JsonProperty("person1IdQ14StartNode") long person1IdQ14StartNode,
        @JsonProperty("person2IdQ14EndNode")   long person2IdQ14EndNode
    )
    {
        this.person1IdQ14StartNode = person1IdQ14StartNode;
        this.person2IdQ14EndNode = person2IdQ14EndNode;
    }

    public LdbcQuery14b( LdbcQuery14b query )
    {
        this.person1IdQ14StartNode = query.getPerson1IdQ14StartNode();
        this.person2IdQ14EndNode = query.getPerson2IdQ14EndNode();
    }

    public long getPerson1IdQ14StartNode()
    {
        return person1IdQ14StartNode;
    }

    public long getPerson2IdQ14EndNode()
    {
        return person2IdQ14EndNode;
    }

    @Override
    public LdbcQuery14b newInstance(){
        return new LdbcQuery14b(this);
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON1_ID, person1IdQ14StartNode)
                .put(PERSON2_ID, person2IdQ14EndNode)
                .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcQuery14b that = (LdbcQuery14b) o;

        if ( person1IdQ14StartNode != that.person1IdQ14StartNode )
        { return false; }
        if ( person2IdQ14EndNode != that.person2IdQ14EndNode )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (person1IdQ14StartNode ^ (person1IdQ14StartNode >>> 32));
        result = 31 * result + (int) (person2IdQ14EndNode ^ (person2IdQ14EndNode >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery14b{" +
               "person1IdQ14StartNode=" + person1IdQ14StartNode +
               ", person2IdQ14EndNode=" + person2IdQ14EndNode +
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
}
