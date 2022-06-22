package org.ldbcouncil.snb.driver.workloads.interactive.queries;
/**
 * LdbcQuery13.java
 * 
 * Interactive workload complex read query 13:
 * -- Single shortest path --
 * 
 * Given two Persons, find the shortest path between these two Persons in
 * the subgraph induced by the knows edges. Return the length of this path:
 * -  −1: no path found
 * -  0: start person = end person
 * -  > 0: path found (start person ≠ end person)
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Map;

public class LdbcQuery13 extends Operation<LdbcQuery13Result>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 13;
    // Parameters used for replacement in queries
    public static final String PERSON1_ID = "person1Id";
    public static final String PERSON2_ID = "person2Id";

    private final long person1IdQ13StartNode;
    private final long person2IdQ13EndNode;

    public LdbcQuery13(
        @JsonProperty("person1IdQ13StartNode") long person1IdQ13StartNode,
        @JsonProperty("person2IdQ13EndNode")   long person2IdQ13EndNode
    )
    {
        this.person1IdQ13StartNode = person1IdQ13StartNode;
        this.person2IdQ13EndNode = person2IdQ13EndNode;
    }

    public LdbcQuery13( LdbcQuery13 query )
    {
        this.person1IdQ13StartNode = query.getPerson1IdQ13StartNode();
        this.person2IdQ13EndNode = query.getPerson2IdQ13EndNode();
    }

    public long getPerson1IdQ13StartNode()
    {
        return person1IdQ13StartNode;
    }

    public long getPerson2IdQ13EndNode()
    {
        return person2IdQ13EndNode;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON1_ID, person1IdQ13StartNode)
                .put(PERSON2_ID, person2IdQ13EndNode)
                .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcQuery13 that = (LdbcQuery13) o;

        if ( person1IdQ13StartNode != that.person1IdQ13StartNode )
        { return false; }
        if ( person2IdQ13EndNode != that.person2IdQ13EndNode )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (person1IdQ13StartNode ^ (person1IdQ13StartNode >>> 32));
        result = 31 * result + (int) (person2IdQ13EndNode ^ (person2IdQ13EndNode >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery13{" +
               "person1IdQ13StartNode=" + person1IdQ13StartNode +
               ", person2IdQ13EndNode=" + person2IdQ13EndNode +
               '}';
    }

    @Override
    public LdbcQuery13Result deserializeResult( String serializedResults ) throws IOException
    {
        LdbcQuery13Result marshaledOperationResult;
        marshaledOperationResult = OBJECT_MAPPER.readValue(serializedResults, LdbcQuery13Result.class);
        return marshaledOperationResult;
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
