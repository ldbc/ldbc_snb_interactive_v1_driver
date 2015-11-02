package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery19StrangerInteraction extends Operation<List<LdbcSnbBiQuery19StrangerInteractionResult>>
{
    public static final int TYPE = 19;
    public static final int DEFAULT_LIMIT = 100;
    private final long date;
    private final String tagClassA;
    private final String tagClassB;
    private final int limit;

    public LdbcSnbBiQuery19StrangerInteraction( long date, String tagClassA, String tagClassB, int limit )
    {
        this.date = date;
        this.tagClassA = tagClassA;
        this.tagClassB = tagClassB;
        this.limit = limit;
    }

    public long date()
    {
        return date;
    }

    public String tagClassA()
    {
        return tagClassA;
    }

    public String tagClassB()
    {
        return tagClassB;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery19StrangerInteraction{" +
               "date=" + date +
               ", tagClassA='" + tagClassA + '\'' +
               ", tagClassB='" + tagClassB + '\'' +
               ", limit=" + limit +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery19StrangerInteraction that = (LdbcSnbBiQuery19StrangerInteraction) o;

        if ( date != that.date )
        { return false; }
        if ( limit != that.limit )
        { return false; }
        if ( tagClassA != null ? !tagClassA.equals( that.tagClassA ) : that.tagClassA != null )
        { return false; }
        return !(tagClassB != null ? !tagClassB.equals( that.tagClassB ) : that.tagClassB != null);

    }

    @Override
    public int hashCode()
    {
        int result = (int) (date ^ (date >>> 32));
        result = 31 * result + (tagClassA != null ? tagClassA.hashCode() : 0);
        result = 31 * result + (tagClassB != null ? tagClassB.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery19StrangerInteractionResult> marshalResult( String serializedResults )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery19StrangerInteractionResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long personId = ((Number) row.get( 0 )).longValue();
            int strangerCount = ((Number) row.get( 1 )).intValue();
            int count = ((Number) row.get( 2 )).intValue();
            result.add(
                    new LdbcSnbBiQuery19StrangerInteractionResult(
                            personId,
                            strangerCount,
                            count
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery19StrangerInteractionResult> result =
                (List<LdbcSnbBiQuery19StrangerInteractionResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery19StrangerInteractionResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.personId() );
            resultFields.add( row.strangerCount() );
            resultFields.add( row.count() );
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
