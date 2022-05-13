package org.ldbcouncil.snb.driver.workloads.interactive;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import org.ldbcouncil.snb.driver.Operation;

public class LdbcUpdateJsonSerializer extends StdSerializer< Operation>{
    
    public LdbcUpdateJsonSerializer() {
        this(null);
    }

    public LdbcUpdateJsonSerializer(Class< Operation> t) {
        super(t);
    }

    @Override
    public void serialize( Operation operation, JsonGenerator generator, SerializerProvider provider) throws IOException {
        generator.writeStartObject();
        generator.writeNumber(LdbcSnbInteractiveWorkloadConfiguration.WRITE_OPERATION_NO_RESULT_DEFAULT_RESULT);
        generator.writeEndObject();
    }
}
