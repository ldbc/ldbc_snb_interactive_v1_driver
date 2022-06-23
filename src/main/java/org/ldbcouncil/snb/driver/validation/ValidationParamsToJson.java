// package org.ldbcouncil.snb.driver.validation;

// import java.io.IOException;
// import java.util.Arrays;
// import java.util.List;

// import com.fasterxml.jackson.databind.ObjectMapper;

// import org.ldbcouncil.snb.driver.Operation;
// import org.ldbcouncil.snb.driver.generator.GeneratorException;
// import static java.lang.String.format;

// public class ValidationParamsToJson
// {
//     private final List<ValidationParam> validationParams;
//     private final boolean performSerializationMarshallingChecks;

//     ObjectMapper OBJECT_MAPPER = new ObjectMapper();

//     public ValidationParamsToJson( List<ValidationParam> validationParams,
//             boolean performSerializationMarshallingChecks )
//     {
//         this.validationParams = validationParams;
//         this.performSerializationMarshallingChecks = performSerializationMarshallingChecks;
//         OBJECT_MAPPER.registerSubtypes(Operation.class, Object.class);
//     }

//     public String serializeValidationParameters()
//     {
//         String serializedValidationParams;
//         try
//         {
//             serializedValidationParams = OBJECT_MAPPER.writeValueAsString(validationParams);
//         }
//         catch ( IOException e )
//         {
//             throw new GeneratorException(
//                     format(
//                             "Workload unable to serialize validationParams\n"
//                             + "validationParams: %s",
//                             validationParams.toString() ),
//                     e );
//         }

//         // Assert that serialization/marshalling is performed correctly
//         if ( performSerializationMarshallingChecks )
//         {
//             Object marshaledOperationResult = null;
//             try
//             {
//                 marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedValidationParams, ValidationParam[].class));
//             }
//             catch (IOException e )
//             {
//                 throw new GeneratorException(
//                         format( ""
//                                 + "Error marshalling serialized validationparam\n"
//                                 + "validationParam: %s\n",
//                                 validationParams ),
//                         e );
//             }
//             if (!marshaledOperationResult.equals( validationParams ) )
//             {
//                 throw new GeneratorException(
//                         format( ""
//                                 + "Operation result and serialized-then-marshaled operation result do not equal\n"
//                                 + "validationParam: %s\n"
//                                 + "serializedValidationParam: %s\n"
//                                 + "marshaledOperationResult: %s",
//                                 validationParams, serializedValidationParams, marshaledOperationResult )
//                 );
//             }
//         }

//         return serializedValidationParams;
//     }
// }
