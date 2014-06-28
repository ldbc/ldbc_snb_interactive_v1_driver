package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LdbcQuery13 extends Operation<List<LdbcQuery13Result>> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final long person1Id;
    private final String person1Uri;
    private final long person2Id;
    private final String person2Uri;

    public LdbcQuery13(long person1Id, String person1Uri, long person2Id, String person2Uri) {
        this.person1Id = person1Id;
        this.person1Uri = person1Uri;
        this.person2Id = person2Id;
        this.person2Uri = person2Uri;
    }

    public long person1Id() {
        return person1Id;
    }

    public String person1Uri() {
        return person1Uri;
    }

    public long person2Id() {
        return person2Id;
    }

    public String person2Uri() {
        return person2Uri;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery13 that = (LdbcQuery13) o;

        if (person1Id != that.person1Id) return false;
        if (person2Id != that.person2Id) return false;
        if (person1Uri != null ? !person1Uri.equals(that.person1Uri) : that.person1Uri != null) return false;
        if (person2Uri != null ? !person2Uri.equals(that.person2Uri) : that.person2Uri != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (person1Id ^ (person1Id >>> 32));
        result = 31 * result + (person1Uri != null ? person1Uri.hashCode() : 0);
        result = 31 * result + (int) (person2Id ^ (person2Id >>> 32));
        result = 31 * result + (person2Uri != null ? person2Uri.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery13{" +
                "person1Id=" + person1Id +
                ", person1Uri='" + person1Uri + '\'' +
                ", person2Id=" + person2Id +
                ", person2Uri='" + person2Uri + '\'' +
                '}';
    }

    @Override
    public List<LdbcQuery13Result> marshalResult(String serializedResults) throws SerializingMarshallingException {
        List<List<Object>> resultsAsList;
        try {
            resultsAsList = objectMapper.readValue(serializedResults, new TypeReference<List<List<Object>>>() {
            });
        } catch (IOException e) {
            throw new SerializingMarshallingException(String.format("Error while parsing serialized results\n%s", serializedResults), e);
        }

        List<LdbcQuery13Result> results = new ArrayList<>();
        for (int i = 0; i < resultsAsList.size(); i++) {
            List<Object> resultAsList = resultsAsList.get(i);
            int shortestPathLength = ((Number) resultAsList.get(0)).intValue();

            results.add(new LdbcQuery13Result(
                    shortestPathLength
            ));
        }

        return results;
    }

    @Override
    public String serializeResult(Object resultsObject) throws SerializingMarshallingException {
        List<LdbcQuery13Result> results = (List<LdbcQuery13Result>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for (int i = 0; i < results.size(); i++) {
            LdbcQuery13Result result = results.get(i);
            List<Object> resultFields = new ArrayList<>();
            resultFields.add(result.shortestPathLength());
            resultsFields.add(resultFields);
        }

        try {
            return objectMapper.writeValueAsString(resultsFields);
        } catch (IOException e) {
            throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", results.toString()), e);
        }
    }
}
