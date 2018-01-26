package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LdbcSnbBiQuery16ExpertsInSocialCircle extends Operation<List<LdbcSnbBiQuery16ExpertsInSocialCircleResult>>
{
    public static final int TYPE = 16;
    public static final int DEFAULT_LIMIT = 100;
    public static final String PERSON_ID = "personId";
    public static final String COUNTRY = "country";
    public static final String TAG_CLASS = "tagClass";
    public static final String MIN_PATH_DISTANCE = "minPathDistance";
    public static final String MAX_PATH_DISTANCE = "maxPathDistance";
    public static final String LIMIT = "limit";

    private final long personId;
    private final String country;
    private final String tagClass;
    private final int minPathDistance;
    private final int maxPathDistance;
    private final int limit;

    public LdbcSnbBiQuery16ExpertsInSocialCircle( long personId, String country, String tagClass, int
            minPathDistance, int maxPathDistance, int limit )
    {
        this.personId = personId;
        this.country = country;
        this.tagClass = tagClass;
        this.minPathDistance = minPathDistance;
        this.maxPathDistance = maxPathDistance;
        this.limit = limit;
    }

    public long personId()
    {
        return personId;
    }

    public String country()
    {
        return country;
    }

    public String tagClass()
    {
        return tagClass;
    }

    public int minPathDistance()
    {
        return minPathDistance;
    }

    public int maxPathDistance()
    {
        return maxPathDistance;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personId)
                .put(COUNTRY, country)
                .put(TAG_CLASS, tagClass)
                .put(MIN_PATH_DISTANCE, minPathDistance)
                .put(MAX_PATH_DISTANCE, maxPathDistance)
                .put(LIMIT, limit)
                .build();
    }

    @Override
    public String toString() {
        return "LdbcSnbBiQuery16ExpertsInSocialCircle{" +
                "personId=" + personId +
                ", country='" + country + '\'' +
                ", tagClass='" + tagClass + '\'' +
                ", minPathDistance=" + minPathDistance +
                ", maxPathDistance=" + maxPathDistance +
                ", limit=" + limit +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcSnbBiQuery16ExpertsInSocialCircle that = (LdbcSnbBiQuery16ExpertsInSocialCircle) o;

        if (personId != that.personId) return false;
        if (minPathDistance != that.minPathDistance) return false;
        if (maxPathDistance != that.maxPathDistance) return false;
        if (limit != that.limit) return false;
        if (country != null ? !country.equals(that.country) : that.country != null) return false;
        return tagClass != null ? tagClass.equals(that.tagClass) : that.tagClass == null;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (country != null ? country.hashCode() : 0);
        result = 31 * result + (tagClass != null ? tagClass.hashCode() : 0);
        result = 31 * result + minPathDistance;
        result = 31 * result + maxPathDistance;
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
            String tagName = (String) row.get( 1 );
            int messageCount = ((Number) row.get( 2 )).intValue();
            result.add(
                    new LdbcSnbBiQuery16ExpertsInSocialCircleResult(
                            personId,
                            tagName,
                            messageCount
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
