package org.ldbcouncil.snb.driver.validation;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.ldbcouncil.snb.driver.Workload;
import org.ldbcouncil.snb.driver.generator.GeneratorException;
import static java.lang.String.format;

public class ValidationParamsToJson
{
    private final List<ValidationParam> validationParams;
    private final boolean performSerializationMarshallingChecks;

    ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public ValidationParamsToJson( List<ValidationParam> validationParams,
        Workload workload,
        boolean performSerializationMarshallingChecks )
    {
        this.validationParams = validationParams;
        this.performSerializationMarshallingChecks = performSerializationMarshallingChecks;
        OBJECT_MAPPER.registerSubtypes(workload.getOperationClass(), Object.class);
    }

    public void serializeValidationParameters(File outputFile) throws IOException
    {
        // // Assert that serialization/marshalling is performed correctly
        if ( this.performSerializationMarshallingChecks )
        {
            String serializedValidationParams;
            try
            {
                serializedValidationParams = OBJECT_MAPPER.writeValueAsString(validationParams);
            }
            catch ( IOException e )
            {
                throw new GeneratorException(
                        format(
                                "Workload unable to serialize validationParams\n"
                                + "validationParams: %s",
                                validationParams.toString() ),
                        e );
            }

            Object deserializedValidationParameters = null;
            try
            {
                deserializedValidationParameters = Arrays.asList(OBJECT_MAPPER.readValue(serializedValidationParams, ValidationParam[].class));
            }
            catch (IOException e )
            {
                throw new GeneratorException(
                    format( ""
                        + "Error marshalling serialized validationparam\n"
                        + "validationParam: %s\n",
                        validationParams ), e
                    );
            }
            if (!deserializedValidationParameters.equals( validationParams ) )
            {
                throw new GeneratorException(
                    format( ""
                        + "Deserialized validation parameters and original validation parameters do not equal\n"
                        + "validationParams: %s\n"
                        + "serializedValidationParam: %s\n"
                        + "deserializedValidationParameters: %s",
                        validationParams,
                        serializedValidationParams,
                        deserializedValidationParameters
                    )
                );
            }
        }

        OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(outputFile, validationParams);
    }
}
