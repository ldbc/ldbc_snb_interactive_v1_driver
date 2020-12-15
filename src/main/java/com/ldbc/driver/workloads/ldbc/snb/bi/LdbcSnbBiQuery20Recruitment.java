package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LdbcSnbBiQuery20Recruitment extends Operation<List<LdbcSnbBiQuery20RecruitmentResult>>
{
    public static final int TYPE = 20;
    public static final int DEFAULT_LIMIT = 20;
    public static final String COMPANY = "company";
    public static final String PERSON2_ID = "person2Id";
    public static final String LIMIT = "limit";

    private final String company;
    private final long person2Id;
    private final int limit;

    public LdbcSnbBiQuery20Recruitment( String company, long person2Id, int limit )
    {
        this.company = company;
        this.person2Id = person2Id;
        this.limit = limit;
    }

    public String company() {
        return company;
    }

    public long person2Id()
    {
        return person2Id;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(COMPANY, company)
                .put(PERSON2_ID, person2Id)
                .put(LIMIT, limit)
                .build();
    }

    @Override
    public String toString() {
        return "LdbcSnbBiQuery20Recruitment{" +
                "company='" + company + '\'' +
                ", person2Id=" + person2Id +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcSnbBiQuery20Recruitment that = (LdbcSnbBiQuery20Recruitment) o;

        if (person2Id != that.person2Id) return false;
        if (limit != that.limit) return false;
        return company != null ? company.equals(that.company) : that.company == null;
    }

    @Override
    public int hashCode() {
        int result = company != null ? company.hashCode() : 0;
        result = 31 * result + (int) (person2Id ^ (person2Id >>> 32));
        result = 31 * result + limit;
        return result;
    }

    @Override
    public List<LdbcSnbBiQuery20RecruitmentResult> marshalResult( String serializedResults ) throws
            SerializingMarshallingException
    {
        List<List<Object>> resultsAsList = SerializationUtil.marshalListOfLists( serializedResults );
        List<LdbcSnbBiQuery20RecruitmentResult> result = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> row = resultsAsList.get( i );
            long person1Id = ((Number) row.get( 0 )).longValue();
            int totalWeight = ((Number) row.get( 1 )).intValue();
            result.add(
                    new LdbcSnbBiQuery20RecruitmentResult( person1Id, totalWeight )
            );
        }
        return result;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
        List<LdbcSnbBiQuery20RecruitmentResult> result =
                (List<LdbcSnbBiQuery20RecruitmentResult>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < result.size(); i++ )
        {
            LdbcSnbBiQuery20RecruitmentResult row = result.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( row.person1Id() );
            resultFields.add( row.totalWeight() );
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
