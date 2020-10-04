package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ldbc.driver.SerializingMarshallingException;

import java.io.IOException;
import java.util.List;

import static java.lang.String.format;

public class SerializationUtil
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference LIST_OF_LISTS_TYPE_REFERENCE =
            new TypeReference<List<List<Object>>>()
            {
            };

    public static synchronized List<List<Object>> marshalListOfLists( String serializedJson )
            throws SerializingMarshallingException
    {
        return marshalListOfLists( serializedJson, LIST_OF_LISTS_TYPE_REFERENCE );
    }

    public static synchronized List<List<Object>> marshalListOfLists( String serializedJson,
            TypeReference typeReference )
            throws SerializingMarshallingException
    {
        try
        {
            return (List<List<Object>>) OBJECT_MAPPER.readValue( serializedJson, typeReference );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException( format( "Error marshalling object\n%s", serializedJson ), e );
        }
    }

    public static synchronized String toJson( Object object ) throws SerializingMarshallingException
    {
        try
        {
            return OBJECT_MAPPER.writeValueAsString( object );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException( format( "Error serializing result\n%s", object.toString() ), e );
        }
    }
}
