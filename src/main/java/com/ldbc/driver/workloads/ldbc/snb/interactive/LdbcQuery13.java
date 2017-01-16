package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.lang.String.format;

public class LdbcQuery13 extends Operation<List<LdbcQuery13Result>>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int TYPE = 13;
    private final long personId;
    private final Date maxDate;

    public LdbcQuery13( long personId, Date maxDate )
    {
	super();
        this.personId = personId;
        this.maxDate = maxDate;
    }

    public long personId()
    {
        return personId;
    }

    public Date maxDate()
    {
        return maxDate;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcQuery13 that = (LdbcQuery13) o;

        if ( personId != that.personId )
        { return false; }
	if ( maxDate != null ? !maxDate.equals( that.maxDate ) : that.maxDate != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
	result = 31 * result + (maxDate != null ? maxDate.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery13{" +
               "personId=" + personId +
               ", maxDate=" + maxDate +
               '}';
    }

    @Override
    public List<LdbcQuery13Result> marshalResult( String serializedResults ) throws SerializingMarshallingException
    {
	List<List<Object>> resultsAsList;
        try
        {
            resultsAsList = OBJECT_MAPPER.readValue( serializedResults, new TypeReference<List<List<Object>>>()
            {
            } );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException(
                    format( "Error while parsing serialized results\n%s", serializedResults ), e );
        }

	List<LdbcQuery13Result> results = new ArrayList<>();
        for ( int i = 0; i < resultsAsList.size(); i++ )
        {
            List<Object> resultAsList = resultsAsList.get( i );
            long personId = ((Number) resultAsList.get( 0 )).longValue();
            String personFirstName = (String) resultAsList.get( 1 );
            String personLastName = (String) resultAsList.get( 2 );
	    long postOrCommentId = ((Number) resultAsList.get( 3 )).longValue();
	    String postOrCommentContent = (String) resultAsList.get( 4 );
            long postOrCommentCreationDate = ((Number) resultAsList.get( 5 )).longValue();

            results.add( new LdbcQuery13Result(
                    personId,
                    personFirstName,
                    personLastName,
                    postOrCommentId,
                    postOrCommentContent,
                    postOrCommentCreationDate
            ) );
	}

        return results;
    }

    @Override
    public String serializeResult( Object resultsObject ) throws SerializingMarshallingException
    {
	List<LdbcQuery13Result> results = (List<LdbcQuery13Result>) resultsObject;
        List<List<Object>> resultsFields = new ArrayList<>();
        for ( int i = 0; i < results.size(); i++ )
        {
            LdbcQuery13Result result = results.get( i );
            List<Object> resultFields = new ArrayList<>();
            resultFields.add( result.personId() );
            resultFields.add( result.personFirstName() );
            resultFields.add( result.personLastName() );
            resultFields.add( result.postOrCommentId() );
            resultFields.add( result.postOrCommentContent() );
            resultFields.add( result.postOrCommentCreationDate() );
            resultsFields.add( resultFields );
        }

        try
        {
            return OBJECT_MAPPER.writeValueAsString( resultsFields );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException(
                    format( "Error while trying to serialize result\n%s", results.toString() ), e );
        }
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
