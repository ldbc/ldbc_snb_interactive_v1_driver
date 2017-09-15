package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery25WeightedPaths extends Operation<List<LdbcSnbBiQuery25WeightedPathsResult>>
{
    public static final int TYPE = 25;
    private final long person1Id;
    private final long person2Id;
    private final long startDate;
    private final long endDate;

    public LdbcSnbBiQuery25WeightedPaths( long person1Id, long person2Id, long startDate, long endDate )
    {
        this.person1Id = person1Id;
        this.person2Id = person2Id;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public long person1Id()
    {
        return person1Id;
    }

    public long person2Id()
    {
        return person2Id;
    }

    public long startDate()
    {
        return startDate;
    }

    public long endDate()
    {
        return endDate;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery25{" +
               "person1Id=" + person1Id +
               ", person2Id=" + person2Id +
               ", startDate=" + startDate +
               ", endDate=" + endDate +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery25WeightedPaths that = (LdbcSnbBiQuery25WeightedPaths) o;

        if ( person1Id != that.person1Id )
        { return false; }
        if ( person2Id != that.person2Id )
        { return false; }
        if ( startDate != that.startDate )
        { return false; }
        if ( endDate != that.endDate )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (person1Id ^ (person1Id >>> 32));
        result = 31 * result + (int) (person2Id ^ (person2Id >>> 32));
        result = 31 * result + (int) (startDate ^ (startDate >>> 32));
        result = 31 * result + (int) (endDate ^ (endDate >>> 32));
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery25WeightedPathsResult> marshalResult( String serializedResults ) throws
            SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfListsLongs( serializedResults );
        List<LdbcSnbBiQuery25WeightedPathsResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            List<Long> personIds = (List<Long>) row.get( 0 );
            result.add(
                    new LdbcSnbBiQuery25WeightedPathsResult( personIds )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery25WeightedPathsResult> result =
                (List<LdbcSnbBiQuery25WeightedPathsResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery25WeightedPathsResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.personIds() );
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
