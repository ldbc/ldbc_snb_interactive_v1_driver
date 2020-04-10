package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.util.ListUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static java.lang.String.*;

public class LdbcDelete4RemoveForum extends Operation<LdbcNoResult>
{
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final int TYPE = 2004;
    public static final String FORUM_ID = "forumId";

    private final long forumId;

    public LdbcDelete4RemoveForum(long forumId)
    {
        this.forumId = forumId;
    }

    public long forumId()
    {
        return forumId;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(FORUM_ID, forumId)
                .build();
    }

    private <T extends Comparable> List<T> sort( List<T> list )
    {
        Collections.sort( list );
        return list;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcDelete4RemoveForum that = (LdbcDelete4RemoveForum) o;

        return forumId == that.forumId;
    }

    @Override
    public int hashCode() {
        return (int) (forumId ^ (forumId >>> 32));
    }

    @Override
    public String toString() {
        return "LdbcDelete4RemoveForum{" +
                "forumId=" + forumId +
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
