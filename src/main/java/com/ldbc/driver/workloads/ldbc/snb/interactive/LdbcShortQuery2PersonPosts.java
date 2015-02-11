package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LdbcShortQuery2PersonPosts extends Operation<List<LdbcShortQuery2PersonPostsResult>> {
    public static final int TYPE = 102;
    public static final int DEFAULT_LIMIT = 10;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final long personId;
    private final int limit;

    public LdbcShortQuery2PersonPosts(long personId, int limit) {
        this.personId = personId;
        this.limit = limit;
    }

    public long personId() {
        return personId;
    }

    public int limit() {
        return limit;
    }

    @Override
    public List<LdbcShortQuery2PersonPostsResult> marshalResult(String serializedResult) throws SerializingMarshallingException {
        List<List<Object>> resultsAsList;
        try {
            resultsAsList = objectMapper.readValue(serializedResult, new TypeReference<List<List<Object>>>() {
            });
        } catch (IOException e) {
            throw new SerializingMarshallingException(String.format("Error while parsing serialized results\n%s", serializedResult), e);
        }

        List<LdbcShortQuery2PersonPostsResult> results = new ArrayList<>();
        for (int i = 0; i < resultsAsList.size(); i++) {
            List<Object> resultAsList = resultsAsList.get(i);

            long postId = ((Number) resultAsList.get(0)).longValue();
            String postContent = (String) resultAsList.get(1);
            long postCreationDate = ((Number) resultAsList.get(2)).longValue();

            results.add(
                    new LdbcShortQuery2PersonPostsResult(
                            postId,
                            postContent,
                            postCreationDate
                    )
            );
        }
        return results;
    }

    @Override
    public String serializeResult(Object operationResultInstance) throws SerializingMarshallingException {
        List<LdbcShortQuery2PersonPostsResult> results = (List<LdbcShortQuery2PersonPostsResult>) operationResultInstance;

        List<List<Object>> resultsFields = new ArrayList<>();
        for (int i = 0; i < results.size(); i++) {
            LdbcShortQuery2PersonPostsResult result = results.get(i);
            List<Object> resultFields = new ArrayList<>();
            resultFields.add(result.postId());
            resultFields.add(result.postContent());
            resultFields.add(result.creationDate());
            resultsFields.add(resultFields);
        }

        try {
            return objectMapper.writeValueAsString(resultsFields);
        } catch (IOException e) {
            throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", results.toString()), e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcShortQuery2PersonPosts that = (LdbcShortQuery2PersonPosts) o;

        if (limit != that.limit) return false;
        if (personId != that.personId) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString() {
        return "LdbcShortQuery2PersonPosts{" +
                "personId=" + personId +
                ", limit=" + limit +
                '}';
    }

    @Override
    public int type() {
        return TYPE;
    }
}