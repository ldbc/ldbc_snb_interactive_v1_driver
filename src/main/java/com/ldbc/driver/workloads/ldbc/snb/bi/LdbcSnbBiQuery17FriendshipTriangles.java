package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery17FriendshipTriangles extends Operation<LdbcSnbBiQuery17FriendshipTrianglesResult>
{
    public static final int TYPE = 17;
    private final String country;

    public LdbcSnbBiQuery17FriendshipTriangles( String country )
    {
        this.country = country;
    }

    public String country()
    {
        return country;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery17FriendshipTriangles{" +
               "country='" + country + '\'' +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery17FriendshipTriangles that = (LdbcSnbBiQuery17FriendshipTriangles) o;

        return !(country != null ? !country.equals( that.country ) : that.country != null);

    }

    @Override
    public int hashCode()
    {
        return country != null ? country.hashCode() : 0;
    }

    @Override
    public LdbcSnbBiQuery17FriendshipTrianglesResult marshalResult( String serializedResults )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<Object> row = resultsAsList.get( 0 );
        int count = ((Number) row.get( 0 )).intValue();
        return new LdbcSnbBiQuery17FriendshipTrianglesResult( count );
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        LdbcSnbBiQuery17FriendshipTrianglesResult result = (LdbcSnbBiQuery17FriendshipTrianglesResult) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        List<Object> resultFields = new ArrayList<>();
        resultFields.add( result.count() );
        resultsFields.add( resultFields );
        return SerializationUtil.toJson( resultsFields );
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
