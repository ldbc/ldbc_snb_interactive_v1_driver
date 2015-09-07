package com.ldbc.driver.validation;

import com.google.common.collect.Lists;
import com.ldbc.driver.Db;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple2;
import com.ldbc.driver.util.Tuple3;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.codehaus.jackson.util.DefaultPrettyPrinter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;

public class DbValidationResult
{
    private final Db db;
    private final Set<Class> missingHandlersForOperationTypes;
    private final List<Tuple2<Operation,String>> unableToExecuteOperations;
    private final List<Tuple3<Operation,Object,Object>> incorrectResultsForOperations;
    private final Map<Class,Integer> successfullyExecutedOperationsPerOperationType;
    private final Map<Class,Integer> totalOperationsPerOperationType;
    private final ObjectMapper objectMapper;
    private static final TypeReference<List<Map<String,Object>>> TYPE_REFERENCE =
            new TypeReference<List<Map<String,Object>>>()
            {
            };
    private final DefaultPrettyPrinter defaultPrettyPrinter;

    DbValidationResult( Db db )
    {
        this.db = db;
        this.missingHandlersForOperationTypes = new HashSet<>();
        this.unableToExecuteOperations = new ArrayList<>();
        this.incorrectResultsForOperations = new ArrayList<>();
        this.successfullyExecutedOperationsPerOperationType = new HashMap<>();
        this.totalOperationsPerOperationType = new HashMap<>();
        this.objectMapper = new ObjectMapper();
        this.defaultPrettyPrinter = new DefaultPrettyPrinter();
        this.defaultPrettyPrinter.indentArraysWith( new DefaultPrettyPrinter.Lf2SpacesIndenter() );
    }

    void reportMissingHandlerForOperation( Operation operation )
    {
        missingHandlersForOperationTypes.add( operation.getClass() );
        incrementOperationCountPerOperationType( operation.getClass() );
    }

    void reportUnableToExecuteOperation( Operation operation, String errorMessage )
    {
        unableToExecuteOperations.add( Tuple.tuple2( operation, errorMessage ) );
        incrementOperationCountPerOperationType( operation.getClass() );
    }

    void reportIncorrectResultForOperation( Operation operation, Object expectedResult, Object actualResult )
    {
        incorrectResultsForOperations.add( Tuple.tuple3( operation, expectedResult, actualResult ) );
        incrementOperationCountPerOperationType( operation.getClass() );
    }

    void reportSuccessfulExecution( Operation operation )
    {
        if ( false == successfullyExecutedOperationsPerOperationType.containsKey( operation.getClass() ) )
        { successfullyExecutedOperationsPerOperationType.put( operation.getClass(), 0 ); }
        int successfullyExecutedOperationsForOperationType =
                successfullyExecutedOperationsPerOperationType.get( operation.getClass() );
        successfullyExecutedOperationsForOperationType++;
        successfullyExecutedOperationsPerOperationType
                .put( operation.getClass(), successfullyExecutedOperationsForOperationType );
        incrementOperationCountPerOperationType( operation.getClass() );
    }

    private void incrementOperationCountPerOperationType( Class operationType )
    {
        Integer count = totalOperationsPerOperationType.get( operationType );
        if ( null == count )
        {
            totalOperationsPerOperationType.put( operationType, 1 );
        }
        else
        {
            totalOperationsPerOperationType.put( operationType, count + 1 );
        }
    }

    public boolean isSuccessful()
    {
        return missingHandlersForOperationTypes.isEmpty() && unableToExecuteOperations.isEmpty() &&
               incorrectResultsForOperations.isEmpty();
    }

    public String actualResultsForFailedOperationsAsJsonString( Workload workload ) throws WorkloadException
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "[" );
        for ( int i = 0; i < incorrectResultsForOperations.size() - 1; i++ )
        {
            Operation operation = incorrectResultsForOperations.get( i )._1();
            Object actualResult = incorrectResultsForOperations.get( i )._3();
            sb.append( operationAndResultAsJsonMapString( operation, actualResult, workload ) ).append( "," );
        }
        if ( incorrectResultsForOperations.size() >= 1 )
        {
            Operation operation = incorrectResultsForOperations.get( incorrectResultsForOperations.size() - 1 )._1();
            Object actualResult = incorrectResultsForOperations.get( incorrectResultsForOperations.size() - 1 )._3();
            sb.append( operationAndResultAsJsonMapString( operation, actualResult, workload ) );
        }
        sb.append( "]" );
        try
        {
            return objectMapper.writer( defaultPrettyPrinter )
                    .writeValueAsString( objectMapper.readValue( sb.toString(), TYPE_REFERENCE ) );
        }
        catch ( IOException e )
        {
            throw new WorkloadException( "Error encountered while trying to pretty print JSON output", e );
        }
    }

    public String expectedResultsForFailedOperationsAsJsonString( Workload workload ) throws WorkloadException
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "[" );
        for ( int i = 0; i < incorrectResultsForOperations.size() - 1; i++ )
        {
            Operation operation = incorrectResultsForOperations.get( i )._1();
            Object expectedResult = incorrectResultsForOperations.get( i )._2();
            sb.append( operationAndResultAsJsonMapString( operation, expectedResult, workload ) ).append( "," );
        }
        if ( incorrectResultsForOperations.size() >= 1 )
        {
            Operation operation = incorrectResultsForOperations.get( incorrectResultsForOperations.size() - 1 )._1();
            Object expectedResult = incorrectResultsForOperations.get( incorrectResultsForOperations.size() - 1 )._2();
            sb.append( operationAndResultAsJsonMapString( operation, expectedResult, workload ) );
        }
        sb.append( "]" );
        try
        {
            return objectMapper.writer( defaultPrettyPrinter )
                    .writeValueAsString( objectMapper.readValue( sb.toString(), TYPE_REFERENCE ) );
        }
        catch ( IOException e )
        {
            throw new WorkloadException( "Error encountered while trying to pretty print JSON output", e );
        }
    }

    private String operationAndResultAsJsonMapString( Operation operation, Object result, Workload workload )
            throws WorkloadException
    {
        String serializedOperation;
        try
        {
            serializedOperation = workload.serializeOperation( operation );
        }
        catch ( SerializingMarshallingException e )
        {
            throw new WorkloadException(
                    format( "Error occurred while serializing operation\nOperation: %s", operation ),
                    e
            );
        }
        String serializedResult;
        try
        {
            serializedResult = operation.serializeResult( result );
        }
        catch ( SerializingMarshallingException e )
        {
            throw new WorkloadException(
                    format( "Error occurred while serializing operation result\nResult: %s", result.toString() ),
                    e
            );
        }
        return "{\"operation\":" + serializedOperation + ",\"result\":" + serializedResult + "}";
    }

    public String resultMessage()
    {
        int padRightDistance = 15;
        StringBuilder sb = new StringBuilder();
        sb.append( "Validation Result: " ).append( (isSuccessful()) ? "PASS" : "FAIL" ).append( "\n" );
        sb.append( "Database: " ).append( db.getClass().getName() ).append( "\n" );
        sb.append( "  ***\n" );
        sb.append( "  Successfully executed " ).append( successfullyExecutedOperationsPerOperationType.size() )
                .append( " operation types\n" );
        for ( Class operationType : sort( totalOperationsPerOperationType.keySet() ) )
        {
            sb.append( "    " ).
                    append( (successfullyExecutedOperationsPerOperationType.containsKey( operationType ))
                            ? successfullyExecutedOperationsPerOperationType.get( operationType ) : 0 ).append( " / " ).
                    append( format( "%1$-" + padRightDistance + "s",
                            totalOperationsPerOperationType.get( operationType ) ) ).
                    append( operationType.getSimpleName() ).
                    append( "\n" );
        }
        sb.append( "  ***\n" );
        sb.append( "  Missing handler implementations for " ).append( missingHandlersForOperationTypes.size() )
                .append( " operation types\n" );
        for ( Class operationType : sort( missingHandlersForOperationTypes ) )
        {
            sb.append( "    " ).append( format( "%1$-" + padRightDistance + "s", operationType.getName() ) )
                    .append( "\n" );
        }
        sb.append( "  ***\n" );
        sb.append( "  Unable to execute " ).append( unableToExecuteOperations.size() ).append( " operations\n" );
        Map<Class,Integer> unableToExecuteOperationsGrouping =
                unableToExecuteOperationsGrouping( unableToExecuteOperations );
        for ( Map.Entry<Class,Integer> failedOperationType : MapUtils
                .sortedEntrySet( unableToExecuteOperationsGrouping ) )
        {
            sb.
                    append( failedOperationType.getKey().getSimpleName() ).
                    append( "               " ).append( failedOperationType.getValue() ).append( "\n" );
        }
        sb.append( "  ***\n" );
        sb.append( "  Incorrect results for " ).append( incorrectResultsForOperations.size() )
                .append( " operations\n" );
        Map<Class,Integer> incorrectResultsForOperationsGrouping =
                incorrectResultsForOperationsGrouping( incorrectResultsForOperations );
        for ( Map.Entry<Class,Integer> failedOperationType : MapUtils
                .sortedEntrySet( incorrectResultsForOperationsGrouping ) )
        {
            sb.
                    append( failedOperationType.getKey().getSimpleName() ).
                    append( "               " ).append( failedOperationType.getValue() ).append( "\n" );
        }
        sb.append( "  ***\n" );
        return sb.toString();
    }

    private <T> List<T> sort( Iterable<T> unsorted )
    {
        List<T> sorted = Lists.newArrayList( unsorted );
        Collections.sort( sorted, new DefaultComparator<T>() );
        return sorted;
    }

    private Map<Class,Integer> unableToExecuteOperationsGrouping(
            List<Tuple2<Operation,String>> unableToExecuteOperations )
    {
        Map<Class,Integer> grouping = new HashMap<>();
        for ( Tuple2<Operation,String> failedOperation : unableToExecuteOperations )
        {
            Class operationType = failedOperation._1().getClass();
            if ( grouping.containsKey( operationType ) )
            {
                int count = grouping.get( operationType );
                grouping.put( operationType, count + 1 );
            }
            else
            {
                grouping.put( operationType, 1 );
            }
        }
        return grouping;
    }

    private Map<Class,Integer> incorrectResultsForOperationsGrouping(
            List<Tuple3<Operation,Object,Object>> incorrectResultsForOperations )
    {
        Map<Class,Integer> grouping = new HashMap<>();
        for ( Tuple3<Operation,Object,Object> failedOperation : incorrectResultsForOperations )
        {
            Class operationType = failedOperation._1().getClass();
            if ( grouping.containsKey( operationType ) )
            {
                int count = grouping.get( operationType );
                grouping.put( operationType, count + 1 );
            }
            else
            {
                grouping.put( operationType, 1 );
            }
        }
        return grouping;
    }

    private static class DefaultComparator<T> implements Comparator<T>
    {
        @Override
        public int compare( T o1, T o2 )
        {
            if ( o1 instanceof Comparable )
            { return ((Comparable) o1).compareTo( o2 ); }
            else
            { return o1.toString().compareTo( o2.toString() ); }
        }
    }
}
