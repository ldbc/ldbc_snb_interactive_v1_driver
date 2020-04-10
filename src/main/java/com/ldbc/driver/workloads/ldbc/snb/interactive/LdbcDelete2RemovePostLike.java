package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import static java.lang.String.*;

public class LdbcDelete2RemovePostLike extends Operation<LdbcNoResult>
{
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final int TYPE = 2002;
    public static final String PERSON_ID = "personId";
    public static final String POST_ID = "postId";

    private final long personId;
    private final long postId;

    public LdbcDelete2RemovePostLike(long personId, long postId )
    {
        this.personId = personId;
        this.postId = postId;
    }

    public long personId()
    {
        return personId;
    }

    public long postId()
    {
        return postId;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personId)
                .put(POST_ID, postId)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcDelete2RemovePostLike that = (LdbcDelete2RemovePostLike) o;

        if (personId != that.personId) return false;
        return postId == that.postId;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (int) (postId ^ (postId >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LdbcDelete2RemovePostLike{" +
                "personId=" + personId +
                ", postId=" + postId +
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
