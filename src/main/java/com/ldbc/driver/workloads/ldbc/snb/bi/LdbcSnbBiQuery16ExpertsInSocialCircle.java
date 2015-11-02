package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;

public class LdbcSnbBiQuery16ExpertsInSocialCircle extends Operation<List<LdbcSnbBiQuery16ExpertsInSocialCircleResult>>
{
    public static final int TYPE = 16;
    public static final int DEFAULT_LIMIT = 100;
    private final long person;
    private final String tagClass;
    private final String country;
    private final int limit;

    public LdbcSnbBiQuery16ExpertsInSocialCircle( long person, String tagClass, String country, int limit )
    {
        this.person = person;
        this.tagClass = tagClass;
        this.country = country;
        this.limit = limit;
    }

    public long person()
    {
        return person;
    }

    public String tagClass()
    {
        return tagClass;
    }

    public String country()
    {
        return country;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery16ExpertsInSocialCircle{" +
               "person=" + person +
               ", tagClass='" + tagClass + '\'' +
               ", country='" + country + '\'' +
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

        LdbcSnbBiQuery16ExpertsInSocialCircle that = (LdbcSnbBiQuery16ExpertsInSocialCircle) o;

        if ( person != that.person )
        { return false; }
        if ( limit != that.limit )
        { return false; }
        if ( tagClass != null ? !tagClass.equals( that.tagClass ) : that.tagClass != null )
        { return false; }
        return !(country != null ? !country.equals( that.country ) : that.country != null);

    }

    @Override
    public int hashCode()
    {
        int result = (int) (person ^ (person >>> 32));
        result = 31 * result + (tagClass != null ? tagClass.hashCode() : 0);
        result = 31 * result + (country != null ? country.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery16ExpertsInSocialCircleResult> marshalResult( String serializedResults )
            throws SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery16ExpertsInSocialCircleResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long personId = ((Number) row.get( 0 )).longValue();
            String tag = (String) row.get( 1 );
            int count = ((Number) row.get( 2 )).intValue();
            result.add(
                    new LdbcSnbBiQuery16ExpertsInSocialCircleResult(
                            personId,
                            tag,
                            count
                    )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery16ExpertsInSocialCircleResult> result =
                (List<LdbcSnbBiQuery16ExpertsInSocialCircleResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery16ExpertsInSocialCircleResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.personId() );
            resultFields.add( row.tag() );
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
