package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class LdbcShortQuery5MessageCreator extends Operation<LdbcShortQuery5MessageCreatorResult>
{
    public static final int TYPE = 105;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final long messageId;

    public LdbcShortQuery5MessageCreator( long messageId )
    {
        this.messageId = messageId;
    }

    public long messageId()
    {
        return messageId;
    }

    @Override
    public LdbcShortQuery5MessageCreatorResult marshalResult( String serializedResult )
            throws SerializingMarshallingException
    {
        List<Object> resultAsList;
        try
        {
            resultAsList = objectMapper.readValue( serializedResult, new TypeReference<List<Object>>()
            {
            } );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException( format( "Error while parsing serialized results\n%s",
                    serializedResult ), e );
        }

        long personId = ((Number) resultAsList.get( 0 )).longValue();
        String firstName = (String) resultAsList.get( 1 );
        String lastName = (String) resultAsList.get( 2 );

        return new LdbcShortQuery5MessageCreatorResult(
                personId,
                firstName,
                lastName
        );
    }

    @Override
    public String serializeResult( Object operationResultInstance ) throws SerializingMarshallingException
    {
        LdbcShortQuery5MessageCreatorResult result = (LdbcShortQuery5MessageCreatorResult) operationResultInstance;
        List<Object> resultFields = new ArrayList<>();
        resultFields.add( result.personId() );
        resultFields.add( result.firstName() );
        resultFields.add( result.lastName() );

        try
        {
            return objectMapper.writeValueAsString( resultFields );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException( format( "Error while trying to serialize result\n%s",
                    result.toString() ), e );
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcShortQuery5MessageCreator that = (LdbcShortQuery5MessageCreator) o;

        if ( messageId != that.messageId )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (messageId ^ (messageId >>> 32));
    }

    @Override
    public String toString()
    {
        return "LdbcShortQuery5MessageCreator{" +
               "messageId=" + messageId +
               '}';
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}