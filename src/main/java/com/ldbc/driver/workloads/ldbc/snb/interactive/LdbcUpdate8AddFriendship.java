package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Date;

import static java.lang.String.format;

public class LdbcUpdate8AddFriendship extends Operation<LdbcNoResult>
{
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final int TYPE = 1008;
    private final long person1Id;
    private final long person2Id;
    private final Date creationDate;

    public LdbcUpdate8AddFriendship( long person1Id, long person2Id, Date creationDate )
    {
        this.person1Id = person1Id;
        this.person2Id = person2Id;
        this.creationDate = creationDate;
    }

    public long person1Id()
    {
        return person1Id;
    }

    public long person2Id()
    {
        return person2Id;
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

        LdbcUpdate8AddFriendship that = (LdbcUpdate8AddFriendship) o;

        if ( person1Id != that.person1Id )
        { return false; }
        if ( person2Id != that.person2Id )
        { return false; }
        if ( creationDate != null ? !creationDate.equals( that.creationDate ) : that.creationDate != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (person1Id ^ (person1Id >>> 32));
        result = 31 * result + (int) (person2Id ^ (person2Id >>> 32));
        result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcUpdate8AddFriendship{" +
               "person1Id=" + person1Id +
               ", person2Id=" + person2Id +
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
