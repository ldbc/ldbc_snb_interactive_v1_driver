package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Date;

import static java.lang.String.format;

public class LdbcUpdate2AddPostLike extends Operation<LdbcNoResult>
{
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final int TYPE = 1002;
    private final long personId;
    private final long postId;
    private final Date creationDate;

    public LdbcUpdate2AddPostLike( long personId, long postId, Date creationDate )
    {
        this.personId = personId;
        this.postId = postId;
        this.creationDate = creationDate;
    }

    public long personId()
    {
        return personId;
    }

    public long postId()
    {
        return postId;
    }

    public Date creationDate()
    {
        return creationDate;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcUpdate2AddPostLike that = (LdbcUpdate2AddPostLike) o;

        if ( personId != that.personId )
        { return false; }
        if ( postId != that.postId )
        { return false; }
        if ( creationDate != null ? !creationDate.equals( that.creationDate ) : that.creationDate != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (int) (postId ^ (postId >>> 32));
        result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcUpdate2AddPostLike{" +
               "personId=" + personId +
               ", postId=" + postId +
               ", creationDate=" + creationDate +
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
