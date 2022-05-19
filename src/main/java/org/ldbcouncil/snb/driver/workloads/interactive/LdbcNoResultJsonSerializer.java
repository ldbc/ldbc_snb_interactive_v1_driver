package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * LdbcNoResultJsonSerializer.java
 * 
 * The LdbcNoResult is serialized to -1, which requires custom serialization.
 */
import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class LdbcNoResultJsonSerializer extends StdSerializer< Object>{
    
    public LdbcNoResultJsonSerializer() {
        this(null);
    }

    public LdbcNoResultJsonSerializer(Class< Object> t) {
        super(t);
    }

    @Override
    public void serialize( Object result, JsonGenerator generator, SerializerProvider provider) throws IOException {
        generator.writeString(Integer.toString(LdbcSnbInteractiveWorkloadConfiguration.WRITE_OPERATION_NO_RESULT_DEFAULT_RESULT));
    }
}
