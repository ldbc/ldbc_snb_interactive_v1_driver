package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LdbcQuery14 extends Operation<List<LdbcQuery14Result>> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final long person1Id;
    private final long person2Id;

    public LdbcQuery14(long person1Id, long person2Id) {
        this.person1Id = person1Id;
        this.person2Id = person2Id;
    }

    public long person1Id() {
        return person1Id;
    }

    public long person2Id() {
        return person2Id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery14 that = (LdbcQuery14) o;

        if (person1Id != that.person1Id) return false;
        if (person2Id != that.person2Id) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (person1Id ^ (person1Id >>> 32));
        result = 31 * result + (int) (person2Id ^ (person2Id >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery14{" +
                "person1Id=" + person1Id +
                ", person2Id=" + person2Id +
                '}';
    }

    @Override
    public List<LdbcQuery14Result> marshalResult(String serializedResults) throws SerializingMarshallingException {
        List<List<Object>> resultsAsList;
        try {
            resultsAsList = objectMapper.readValue(
                    serializedResults,
                    new TypeReference<List<List<Object>>>() {
                    }
            );
        } catch (IOException e) {
            throw new SerializingMarshallingException(String.format("Error while parsing serialized results\n%s", serializedResults), e);
        }

        List<LdbcQuery14Result> results = new ArrayList<>();
        for (int i = 0; i < resultsAsList.size(); i++) {
            List<Object> resultAsList = resultsAsList.get(i);
            Iterable<Long> personsIdsInPath = Iterables.transform((List<Number>) resultAsList.get(0), new Function<Number, Long>() {
                @Override
                public Long apply(Number number) {
                    return number.longValue();
                }
            });
            double pathWeight = ((Number) resultAsList.get(1)).doubleValue();

            results.add(
                    new LdbcQuery14Result(
                            personsIdsInPath,
                            pathWeight
                    )
            );
        }
        return results;
    }

    @Override
    public String serializeResult(Object resultsObject) throws SerializingMarshallingException {
        List<LdbcQuery14Result> results = (List<LdbcQuery14Result>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for (int i = 0; i < results.size(); i++) {
            LdbcQuery14Result result = results.get(i);
            List<Object> resultFields = new ArrayList<>();
            resultFields.add(result.personsIdsInPath());
            resultFields.add(result.pathWeight());
            resultsFields.add(resultFields);
        }

        try {
            return objectMapper.writeValueAsString(resultsFields);
        } catch (IOException e) {
            throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", results.toString()), e);
        }
    }

    @Override
    public int type() {
        return 14;
    }
}
