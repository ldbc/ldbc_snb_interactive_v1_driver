package com.ldbc.driver.workloads.simple;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.MinMaxGenerator;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple2;
import com.ldbc.driver.util.Tuple3;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public class SimpleWorkload extends Workload
{
    // NOTE, in a real Workload these would ideally come from configuration and get set in onInit()
    final String TABLE = "usertable";
    final String KEY_NAME_PREFIX = "user";
    final String FIELD_NAME_PREFIX = "field";
    final int NUMBER_OF_FIELDS_IN_RECORD = 10;
    final int NUMBER_OF_FIELDS_TO_READ = 1;
    final int NUMBER_OF_FIELDS_TO_UPDATE = 1;
    final int MIN_SCAN_LENGTH = 1;
    final int MAX_SCAN_LENGTH = 1000;

    final double READ_RATIO = 0.20;
    final double UPDATE_RATIO = 0.20;
    final double INSERT_RATIO = 0.20;
    final double SCAN_RATIO = 0.20;
    final double READ_MODIFY_WRITE_RATIO = 0.20;

    final long INITIAL_INSERT_COUNT = 10;

    @Override
    public Map<Integer,Class<? extends Operation>> operationTypeToClassMapping()
    {
        Map<Integer,Class<? extends Operation>> operationTypeToClassMapping = new HashMap<>();
        operationTypeToClassMapping.put( InsertOperation.TYPE, InsertOperation.class );
        operationTypeToClassMapping.put( ReadModifyWriteOperation.TYPE, ReadModifyWriteOperation.class );
        operationTypeToClassMapping.put( ReadOperation.TYPE, ReadOperation.class );
        operationTypeToClassMapping.put( ScanOperation.TYPE, ScanOperation.class );
        operationTypeToClassMapping.put( UpdateOperation.TYPE, UpdateOperation.class );
        return operationTypeToClassMapping;
    }

    @Override
    public void onInit( Map<String,String> params )
    {
    }

    @Override
    public WorkloadStreams getStreams( GeneratorFactory gf, boolean hasDbConnected ) throws WorkloadException
    {
        long workloadStartTimeAsMilli = 0;

        /**
         * **************************
         *
         * Initial Insert Operation Generator
         *
         * **************************
         */
        // Load Insert Keys
        MinMaxGenerator<Long> insertKeyGenerator = gf.minMaxGenerator( gf.incrementing( 0l, 1l ), 0l, 0l );

        // Insert Fields: Names & Values
        Iterator<Long> fieldValueLengthGenerator = gf.uniform( 1l, 100l );
        Iterator<Iterator<Byte>> randomFieldValueGenerator = gf.sizedUniformBytesGenerator( fieldValueLengthGenerator );
        List<Tuple3<Double,String,Iterator<Iterator<Byte>>>> valuedFields = new ArrayList<>();
        for ( int i = 0; i < NUMBER_OF_FIELDS_IN_RECORD; i++ )
        {
            valuedFields.add( Tuple.tuple3( 1d, FIELD_NAME_PREFIX + i, randomFieldValueGenerator ) );
        }
        Iterator<Map<String,Iterator<Byte>>> insertValuedFieldGenerator =
                gf.weightedDiscreteMap( valuedFields, NUMBER_OF_FIELDS_IN_RECORD );

        Iterator<Operation> initialInsertOperationGenerator = gf.limit(
                new InsertOperationGenerator( TABLE, gf.prefix( insertKeyGenerator, KEY_NAME_PREFIX ),
                        insertValuedFieldGenerator ),
                INITIAL_INSERT_COUNT
        );

        /**
         * **************************
         *
         * Insert Operation Generator
         *
         * **************************
         */
        // Transaction Insert Keys
        InsertOperationGenerator transactionalInsertOperationGenerator = new InsertOperationGenerator(
                TABLE,
                gf.prefix( insertKeyGenerator, KEY_NAME_PREFIX ),
                insertValuedFieldGenerator
        );

        /**
         * **************************
         *
         * Read Operation Generator
         *
         * **************************
         */
        // Read/Update Keys
        Iterator<String> requestKeyGenerator =
                gf.prefix( gf.dynamicRangeUniform( insertKeyGenerator ), KEY_NAME_PREFIX );

        // Read Fields: Names
        List<Tuple2<Double,String>> fields = new ArrayList<>();
        for ( int i = 0; i < NUMBER_OF_FIELDS_IN_RECORD; i++ )
        {
            fields.add( Tuple.tuple2( 1d, FIELD_NAME_PREFIX + i ) );
        }

        Iterator<List<String>> readFieldsGenerator = gf.weightedDiscreteList( fields, NUMBER_OF_FIELDS_TO_READ );

        ReadOperationGenerator readOperationGenerator = new ReadOperationGenerator(
                TABLE,
                requestKeyGenerator,
                readFieldsGenerator );

        /**
         * **************************
         *
         * Update Operation Generator
         *
         * **************************
         */
        // Update Fields: Names & Values
        Iterator<Map<String,Iterator<Byte>>> updateValuedFieldsGenerator = gf.weightedDiscreteMap(
                valuedFields,
                NUMBER_OF_FIELDS_TO_UPDATE );

        UpdateOperationGenerator updateOperationGenerator = new UpdateOperationGenerator(
                TABLE,
                requestKeyGenerator,
                updateValuedFieldsGenerator );

        /**
         * **************************
         *
         * Scan Operation Generator
         *
         * **************************
         */
        // Scan Fields: Names & Values
        Iterator<List<String>> scanFieldsGenerator = gf.weightedDiscreteList( fields, NUMBER_OF_FIELDS_TO_READ );

        // Scan Length: Number of Records
        Iterator<Integer> scanLengthGenerator = gf.uniform( MIN_SCAN_LENGTH, MAX_SCAN_LENGTH );

        ScanOperationGenerator scanOperationGenerator = new ScanOperationGenerator(
                TABLE,
                requestKeyGenerator,
                scanLengthGenerator,
                scanFieldsGenerator );

        /**
         * **************************
         *
         * ReadModifyWrite Operation Generator
         *
         * **************************
         */
        ReadModifyWriteOperationGenerator readModifyWriteOperationGenerator = new ReadModifyWriteOperationGenerator(
                TABLE, requestKeyGenerator, readFieldsGenerator, updateValuedFieldsGenerator );

        /**
         * **************************
         *
         * Transactional Workload Operations
         *
         * **************************
         */
        // proportion of transactions reads/update/insert/scan/read-modify-write
        List<Tuple2<Double,Iterator<Operation>>> operations = new ArrayList<>();
        operations.add( Tuple.tuple2( READ_RATIO, (Iterator<Operation>) readOperationGenerator ) );
        operations.add( Tuple.tuple2( UPDATE_RATIO, (Iterator<Operation>) updateOperationGenerator ) );
        operations.add( Tuple.tuple2( INSERT_RATIO, (Iterator<Operation>) transactionalInsertOperationGenerator ) );
        operations.add( Tuple.tuple2( SCAN_RATIO, (Iterator<Operation>) scanOperationGenerator ) );
        operations.add( Tuple
                .tuple2( READ_MODIFY_WRITE_RATIO, (Iterator<Operation>) readModifyWriteOperationGenerator ) );

        Iterator<Operation> transactionalOperationGenerator = gf.weightedDiscreteDereferencing( operations );

        // iterates initialInsertOperationGenerator before starting with transactionalInsertOperationGenerator
        Iterator<Operation> workloadOperations =
                Iterators.concat( initialInsertOperationGenerator, transactionalOperationGenerator );

        Iterator<Long> startTimesAsMilli = gf.incrementing( workloadStartTimeAsMilli + 1, 100l );
        Iterator<Long> dependencyTimesAsMilli = gf.constant( workloadStartTimeAsMilli );

        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                Sets.<Class<? extends Operation>>newHashSet(),
                Sets.<Class<? extends Operation>>newHashSet(),
                Collections.<Operation>emptyIterator(),
                gf.assignDependencyTimes( dependencyTimesAsMilli,
                        gf.assignStartTimes( startTimesAsMilli, workloadOperations ) ),
                null
        );
        return workloadStreams;
    }

    @Override
    protected void onClose() throws IOException
    {
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference TYPE_REFERENCE = new TypeReference<List<Object>>()
    {
    };

    @Override
    public String serializeOperation( Operation operation ) throws SerializingMarshallingException
    {
        try
        {
            switch ( operation.type() )
            {
            case InsertOperation.TYPE:
            {
                InsertOperation simpleOperation = (InsertOperation) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( simpleOperation.getClass().getName() );
                operationAsList.add( simpleOperation.table() );
                operationAsList.add( simpleOperation.key() );
                // TODO
//                operationAsList.add( simpleOperation.fields() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case ReadOperation.TYPE:
            {
                ReadOperation simpleOperation = (ReadOperation) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( simpleOperation.getClass().getName() );
                operationAsList.add( simpleOperation.table() );
                operationAsList.add( simpleOperation.key() );
                operationAsList.add( simpleOperation.fields() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case UpdateOperation.TYPE:
            {
                UpdateOperation simpleOperation = (UpdateOperation) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( simpleOperation.getClass().getName() );
                operationAsList.add( simpleOperation.table() );
                operationAsList.add( simpleOperation.key() );
                // TODO
//                operationAsList.add( simpleOperation.fields() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case ScanOperation.TYPE:
            {
                ScanOperation simpleOperation = (ScanOperation) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( simpleOperation.getClass().getName() );
                operationAsList.add( simpleOperation.table() );
                operationAsList.add( simpleOperation.startkey() );
                operationAsList.add( simpleOperation.recordCount() );
                operationAsList.add( simpleOperation.fields() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case ReadModifyWriteOperation.TYPE:
            {
                ReadModifyWriteOperation simpleOperation = (ReadModifyWriteOperation) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( simpleOperation.getClass().getName() );
                operationAsList.add( simpleOperation.table() );
                operationAsList.add( simpleOperation.key() );
                operationAsList.add( simpleOperation.fields() );
                // TODO
//                operationAsList.add( simpleOperation.values() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            default:
            {
                throw new SerializingMarshallingException(
                        format(
                                "Workload does not know how to serialize operation\nWorkload: %s\nOperation Type: " +
                                "%s\nOperation: %s",
                                getClass().getName(),
                                operation.getClass().getName(),
                                operation ) );
            }
            }
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException( format( "Error serializing operation\n%s", operation ), e );
        }
    }

    @Override
    public Operation marshalOperation( String serializedOperation ) throws SerializingMarshallingException
    {
        List<Object> operationAsList;
        try
        {
            operationAsList = OBJECT_MAPPER.readValue( serializedOperation, TYPE_REFERENCE );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException(
                    format( "Error while parsing serialized results\n%s", serializedOperation ), e );
        }
        String operationClassName = (String) operationAsList.get( 0 );

        if ( operationClassName.equals( InsertOperation.class.getName() ) )
        {
            return new InsertOperation(
                    (String) operationAsList.get( 1 ),
                    (String) operationAsList.get( 2 ),
                    // TODO
                    new HashMap<String,Iterator<Byte>>()
            );
        }
        else if ( operationClassName.equals( ReadOperation.class.getName() ) )
        {
            return new ReadOperation(
                    (String) operationAsList.get( 1 ),
                    (String) operationAsList.get( 2 ),
                    (List<String>) operationAsList.get( 3 )
            );
        }
        else if ( operationClassName.equals( UpdateOperation.class.getName() ) )
        {
            return new UpdateOperation(
                    (String) operationAsList.get( 1 ),
                    (String) operationAsList.get( 2 ),
                    // TODO
                    new HashMap<String,Iterator<Byte>>()
            );
        }
        else if ( operationClassName.equals( ScanOperation.class.getName() ) )
        {
            return new ScanOperation(
                    (String) operationAsList.get( 1 ),
                    (String) operationAsList.get( 2 ),
                    ((Number) operationAsList.get( 3 )).intValue(),
                    (List<String>) operationAsList.get( 4 )
            );
        }
        else if ( operationClassName.equals( ReadModifyWriteOperation.class.getName() ) )
        {
            return new ReadModifyWriteOperation(
                    (String) operationAsList.get( 1 ),
                    (String) operationAsList.get( 2 ),
                    (List<String>) operationAsList.get( 3 ),
                    // TODO
                    new HashMap<String,Iterator<Byte>>()
            );
        }

        throw new SerializingMarshallingException(
                format(
                        "Workload does not know how to marshal operation\nWorkload: %s\nAssumed Operation Type: " +
                        "%s\nSerialized Operation: %s",
                        getClass().getName(),
                        operationClassName,
                        serializedOperation ) );
    }

    @Override
    public boolean resultsEqual( Operation operation, Object result1, Object result2 ) throws WorkloadException
    {
        if ( null == result1 || null == result2 )
        { return false; }
        else
        { return result1.equals( result2 ); }
    }
}

