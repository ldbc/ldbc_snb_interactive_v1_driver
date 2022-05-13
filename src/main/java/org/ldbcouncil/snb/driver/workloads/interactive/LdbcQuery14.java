package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LdbcQuery14 extends Operation<List<LdbcQuery14Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 14;
    public static final String PERSON1_ID = "person1Id";
    public static final String PERSON2_ID = "person2Id";

    private final long person1Id;
    private final long person2Id;

    public LdbcQuery14( long person1Id, long person2Id )
    {
        this.person1Id = person1Id;
        this.person2Id = person2Id;
    }

    public long getPerson1Id()
    {
        return person1Id;
    }

    public long getPerson2Id()
    {
        return person2Id;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON1_ID, person1Id)
                .put(PERSON2_ID, person2Id)
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

        if ( person1Id != that.person1Id )
        { return false; }
        if ( person2Id != that.person2Id )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (person1Id ^ (person1Id >>> 32));
        result = 31 * result + (int) (person2Id ^ (person2Id >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery14{" +
               "person1Id=" + person1Id +
               ", person2Id=" + person2Id +
               '}';
    }

    @Override
    public List<LdbcQuery14Result> deserializeResult( String serializedResults ) throws IOException
    {
        List<LdbcQuery14Result> marshaledOperationResult;
        marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery14Result[].class));
        return marshaledOperationResult;
    }

    // @Override
    // public List<LdbcQuery14Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
    // {
    //     List<List<Object>> resultsAsList;
    //     try
    //     {
    //         resultsAsList = OBJECT_MAPPER.readValue(
    //                 serializedResults,
    //                 new TypeReference<List<List<Object>>>()
    //                 {
    //                 }
    //         );
    //     }
    //     catch ( IOException e )
    //     {
    //         throw new SerializingMarshallingException(
    //                 format( "Error while parsing serialized results\n%s", serializedResults ), e );
    //     }

    //     List<LdbcQuery14Result> results = new ArrayList<>();
    //     for ( int i = 0; i < resultsAsList.size(); i++ )
    //     {
    //         List<Object> resultAsList = resultsAsList.get( i );
    //         Iterable<Long> personsIdsInPath =
    //                 Iterables.transform( (List<Number>) resultAsList.get( 0 ), new Function<Number,Long>()
    //                 {
    //                     @Override
    //                     public Long apply( Number number )
    //                     {
    //                         return number.longValue();
    //                     }
    //                 } );
    //         double pathWeight = ((Number) resultAsList.get( 1 )).doubleValue();

    //         results.add(
    //                 new LdbcQuery14Result(
    //                         personsIdsInPath,
    //                         pathWeight
    //                 )
    //         );
    //     }
    //     return results;
    // }

    @Override
    public int type()
    {
        return TYPE;
    }
}
