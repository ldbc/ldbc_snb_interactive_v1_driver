package org.ldbcouncil.snb.driver.validation;

import java.io.IOException;

import org.ldbcouncil.snb.driver.Operation;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class ValidationParamSerializer extends StdSerializer< Object> {
    
    public ValidationParamSerializer() {
        this(null);
    }

    public ValidationParamSerializer(Class< Object> t) {
        super(t);
    }

    @Override
    public void serialize( Object result, JsonGenerator generator, SerializerProvider provider) throws IOException {
        ValidationParam param = (ValidationParam) result;
        Operation operation = param.operation();
        Object operationResult = param.operationResult();
        generator.writeStartObject();
        generator.writeObjectField("operation", operation);
        generator.writeObjectField("operationResult", operationResult);
        generator.writeEndObject();
    }
}
