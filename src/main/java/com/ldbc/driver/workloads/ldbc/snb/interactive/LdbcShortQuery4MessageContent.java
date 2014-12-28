package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LdbcShortQuery4MessageContent extends Operation<LdbcShortQuery4MessageContentResult> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final long messageId;

    public LdbcShortQuery4MessageContent(long messageId) {
        this.messageId = messageId;
    }

    public long messageId() {
        return messageId;
    }

    @Override
    public LdbcShortQuery4MessageContentResult marshalResult(String serializedResult) throws SerializingMarshallingException {
        List<Object> resultAsList;
        try {
            resultAsList = objectMapper.readValue(serializedResult, new TypeReference<List<Object>>() {
            });
        } catch (IOException e) {
            throw new SerializingMarshallingException(String.format("Error while parsing serialized results\n%s", serializedResult), e);
        }

        String messageContent = (String) resultAsList.get(0);

        return new LdbcShortQuery4MessageContentResult(
                messageContent
        );
    }

    @Override
    public String serializeResult(Object operationResultInstance) throws SerializingMarshallingException {
        LdbcShortQuery4MessageContentResult result = (LdbcShortQuery4MessageContentResult) operationResultInstance;
        List<Object> resultFields = new ArrayList<>();
        resultFields.add(result.messageContent());
        try {
            return objectMapper.writeValueAsString(resultFields);
        } catch (IOException e) {
            throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", result.toString()), e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcShortQuery4MessageContent that = (LdbcShortQuery4MessageContent) o;

        if (messageId != that.messageId) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (messageId ^ (messageId >>> 32));
    }

    @Override
    public String toString() {
        return "LdbcShortQuery4MessageContent{" +
                "messageId=" + messageId +
                '}';
    }

    @Override
    public int type() {
        return 104;
    }
}