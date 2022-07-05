package org.ldbcouncil.snb.driver.validation;

import java.io.IOException;

import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcOperation;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class ValidationParamDeserializer extends StdDeserializer<ValidationParam> {

    private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public ValidationParamDeserializer() { 
        this(null); 
    } 

    public ValidationParamDeserializer(Class vc) {
        super(vc);
    }

    @Override
    public ValidationParam deserialize(JsonParser jp, DeserializationContext ctxt) 
        throws IOException, JsonProcessingException 
    {
        JsonNode node = jp.getCodec().readTree(jp);
        JsonNode operationNode = node.path("operation");
        JsonNode resultNode = node.path("operationResult");
        // TODO: Remove dependency on LdbcOperation valueType. This should be passed or set in a generic way
        Operation operation = OBJECT_MAPPER.readValue( operationNode.toPrettyString(), LdbcOperation.class);
        Object operationResult = operation.deserializeResult( resultNode.toPrettyString() );
        return ValidationParam.createTyped(operation, operationResult);
    }
}
