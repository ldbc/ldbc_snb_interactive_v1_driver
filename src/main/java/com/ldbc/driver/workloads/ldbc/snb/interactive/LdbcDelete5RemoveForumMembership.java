package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import static java.lang.String.*;

public class LdbcDelete5RemoveForumMembership extends Operation<LdbcNoResult>
{
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final int TYPE = 2005;
    public static final String FORUM_ID = "forumId";
    public static final String PERSON_ID = "personId";

    private final long forumId;
    private final long personId;

    public LdbcDelete5RemoveForumMembership(long forumId, long personId)
    {
        this.forumId = forumId;
        this.personId = personId;
    }

    public long forumId()
    {
        return forumId;
    }

    public long personId()
    {
        return personId;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(FORUM_ID, forumId)
                .put(PERSON_ID, personId)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcDelete5RemoveForumMembership that = (LdbcDelete5RemoveForumMembership) o;

        if (forumId != that.forumId) return false;
        return personId == that.personId;
    }

    @Override
    public int hashCode() {
        int result = (int) (forumId ^ (forumId >>> 32));
        result = 31 * result + (int) (personId ^ (personId >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LdbcDelete5RemoveForumMembership{" +
                "forumId=" + forumId +
                ", personId=" + personId +
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
