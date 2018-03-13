package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import static java.lang.String.format;

public class LdbcUpdate5AddForumMembership extends Operation<LdbcNoResult>
{
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final int TYPE = 1005;
    public static final String FORUM_ID = "forumId";
    public static final String PERSON_ID = "personId";
    public static final String JOIN_DATE = "joinDate";

    private final long forumId;
    private final long personId;
    private final Date joinDate;

    public LdbcUpdate5AddForumMembership( long forumId, long personId, Date joinDate )
    {
        this.forumId = forumId;
        this.personId = personId;
        this.joinDate = joinDate;
    }

    public long forumId()
    {
        return forumId;
    }

    public long personId()
    {
        return personId;
    }

    public Date joinDate()
    {
        return joinDate;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(FORUM_ID, forumId)
                .put(PERSON_ID, personId)
                .put(JOIN_DATE, joinDate)
                .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcUpdate5AddForumMembership that = (LdbcUpdate5AddForumMembership) o;

        if ( forumId != that.forumId )
        { return false; }
        if ( personId != that.personId )
        { return false; }
        if ( joinDate != null ? !joinDate.equals( that.joinDate ) : that.joinDate != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (forumId ^ (forumId >>> 32));
        result = 31 * result + (int) (personId ^ (personId >>> 32));
        result = 31 * result + (joinDate != null ? joinDate.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcUpdate5AddForumMembership{" +
               "forumId=" + forumId +
               ", personId=" + personId +
               ", joinDate=" + joinDate +
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
