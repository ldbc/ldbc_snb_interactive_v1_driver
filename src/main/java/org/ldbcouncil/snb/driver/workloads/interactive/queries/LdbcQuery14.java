package org.ldbcouncil.snb.driver.workloads.interactive.queries;
/**
 * LdbcQuery14.java
 * 
 * Interactive workload complex read query 14:
 * -- Trusted connection paths--
 * 
 * Given two Persons, find all (unweighted) shortest paths between these
 * two Persons, in the subgraph induced by the knows relationship. Then,
 * for each path calculate a weight. The nodes in the path are Persons,
 * and the weight of a path is the sum of weights between every pair of
 * consecutive Person nodes in the path. The weight for a pair of Persons
 * is calculated based on their interactions:
 * - Every direct reply (by one of the Persons) to a Post (by the other Person) contributes 1.0.
 * - Every direct reply (by one of the Persons) to a Comment (by the other Person) contributes 0.5.
 * 
 * Note that interactions are counted both ways (e.g. if Alice writes 2 Post replies and 
 * 1 Comment reply to Bob, while Bob writes 3 Post replies and 4 Comment replies to Alice, 
 * their interaction score is 2 × 1.0 + 1 × 0.5 + 3 × 1.0 + 4 × 0.5 = 7.5).
 * Return all the paths with shortest length, and their weights. Do not return any rows
 * if there is no path between the two Persons
 */


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import org.ldbcouncil.snb.driver.workloads.interactive.LdbcOperation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LdbcQuery14 extends LdbcOperation<List<LdbcQuery14Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 14;
    // Parameters used for replacement in queries
    public static final String PERSON1_ID = "person1Id";
    public static final String PERSON2_ID = "person2Id";

    private final long person1IdQ14StartNode;
    private final long person2IdQ14EndNode;

    public LdbcQuery14(
        @JsonProperty("person1IdQ14StartNode") long person1IdQ14StartNode,
        @JsonProperty("person2IdQ14EndNode")   long person2IdQ14EndNode
    )
    {
        this.person1IdQ14StartNode = person1IdQ14StartNode;
        this.person2IdQ14EndNode = person2IdQ14EndNode;
    }

    public LdbcQuery14( LdbcQuery14 query )
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
    public LdbcQuery14 newInstance(){
        return new LdbcQuery14(this);
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

        LdbcQuery14 that = (LdbcQuery14) o;

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
        return "LdbcQuery14{" +
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
