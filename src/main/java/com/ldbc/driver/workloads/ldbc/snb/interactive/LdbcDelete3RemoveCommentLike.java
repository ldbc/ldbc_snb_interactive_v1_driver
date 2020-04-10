package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.io.IOException;
import java.util.Map;

import static java.lang.String.*;

public class LdbcDelete3RemoveCommentLike extends Operation<LdbcNoResult>
{
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final int TYPE = 2003;
    public static final String PERSON_ID = "personId";
    public static final String COMMENT_ID = "commentId";

    private final long personId;
    private final long commentId;

    public LdbcDelete3RemoveCommentLike(long personId, long commentId)
    {
        this.personId = personId;
        this.commentId = commentId;
    }

    public long personId()
    {
        return personId;
    }

    public long commentId()
    {
        return commentId;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personId)
                .put(COMMENT_ID, commentId)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcDelete3RemoveCommentLike that = (LdbcDelete3RemoveCommentLike) o;

        if (personId != that.personId) return false;
        return commentId == that.commentId;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (int) (commentId ^ (commentId >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LdbcDelete3RemovePostLike{" +
                "personId=" + personId +
                ", commentId=" + commentId +
                '}';
    }

    @Override
    public LdbcNoResult marshalResult( String serializedOperationResult )
    {
        return LdbcNoResult.INSTANCE;
    }

    @Override
    public String serializeResult( Object operationResultInstance ) throws SerializingMarshallingException
    {
        try
        {
            return objectMapper.writeValueAsString(
                    LdbcSnbInteractiveWorkloadConfiguration.WRITE_OPERATION_NO_RESULT_DEFAULT_RESULT );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException( format( "Error while trying to serialize result\n%s",
                    operationResultInstance ), e );
        }
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
