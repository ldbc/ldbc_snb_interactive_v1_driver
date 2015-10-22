package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery3TagEvolution extends Operation<List<LdbcSnbBiQuery3TagEvolutionResult>>
{
    public static final int TYPE = 3;
    public static final int DEFAULT_LIMIT = 100;
    private final long range1Start;
    private final long range1End;
    private final long range2Start;
    private final long range2End;
    private final int limit;

    public LdbcSnbBiQuery3TagEvolution( long range1Start, long range1End, long range2Start, long range2End, int limit )
    {
        this.range1Start = range1Start;
        this.range1End = range1End;
        this.range2Start = range2Start;
        this.range2End = range2End;
        this.limit = limit;
    }

    public long range1Start()
    {
        return range1Start;
    }

    public long range1End()
    {
        return range1End;
    }

    public long range2Start()
    {
        return range2Start;
    }

    public long range2End()
    {
        return range2End;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery3TagEvolution{" +
               "range1Start=" + range1Start +
               ", range1End=" + range1End +
               ", range2Start=" + range2Start +
               ", range2End=" + range2End +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery3TagEvolution that = (LdbcSnbBiQuery3TagEvolution) o;

        if ( range1Start != that.range1Start )
        { return false; }
        if ( range1End != that.range1End )
        { return false; }
        if ( range2Start != that.range2Start )
        { return false; }
        return range2End == that.range2End;

    }

    @Override
    public int hashCode()
    {
        int result = (int) (range1Start ^ (range1Start >>> 32));
        result = 31 * result + (int) (range1End ^ (range1End >>> 32));
        result = 31 * result + (int) (range2Start ^ (range2Start >>> 32));
        result = 31 * result + (int) (range2End ^ (range2End >>> 32));
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery3TagEvolutionResult> marshalResult( String serializedResults )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery3TagEvolutionResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            String tag = (String) row.get( 0 );
            int countA = ((Number) row.get( 1 )).intValue();
            int countB = ((Number) row.get( 2 )).intValue();
            int difference = ((Number) row.get( 3 )).intValue();
            result.add(
                    new LdbcSnbBiQuery3TagEvolutionResult(
                            tag,
                            countA,
                            countB,
                            difference
                    )
            );
        }

        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery3TagEvolutionResult> result = (List<LdbcSnbBiQuery3TagEvolutionResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery3TagEvolutionResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.tag() );
            resultFields.add( row.countA() );
            resultFields.add( row.countB() );
            resultFields.add( row.difference() );
            resultsFields.add( resultFields );
        }
        return SerializationUtil.toJson( resultsFields );
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
