package com.ldbc.driver.workloads.simple;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.ldbc.driver.Client;
import com.ldbc.driver.Operation;
import com.ldbc.driver.data.ByteIterator;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorBuilder;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.generator.MinMaxGeneratorWrapper;
import com.ldbc.driver.generator.PrefixGeneratorWrapper;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.util.Pair;
import com.ldbc.driver.util.Triple;
import com.ldbc.driver.workloads.Workload;
import com.ldbc.driver.workloads.WorkloadException;

public class SimpleWorkload extends Workload
{
    final String TABLE = "usertable";
    final String KEY_NAME_PREFIX = "user";
    final String FIELD_NAME_PREFIX = "field";
    final int NUMBER_OF_FIELDS_IN_RECORD = 10;
    final int NUMBER_OF_FIELDS_TO_READ = 1;
    final int NUMBER_OF_FIELDS_TO_UPDATE = 1;
    final int MIN_SCAN_LENGTH = 1;
    final int MAX_SCAN_LENGTH = 1000;

    final double READ_RATIO = 0.95;
    final double UPDATE_RATIO = 0.05;
    final double INSERT_RATIO = 0.00;
    final double SCAN_RATIO = 0.00;
    final double READ_MODIFY_WRITE_RATIO = 0.00;

    long insertStart;
    long recordCount;

    @Override
    public void onInit( Map<String, String> properties )
    {
        // TODO move this logic to Client and make available via local getters?
        recordCount = Long.parseLong( properties.get( Client.RECORD_COUNT ) );
        insertStart = Long.parseLong( MapUtils.mapGetDefault( properties, Client.INSERT_START,
                Client.INSERT_START_DEFAULT ) );
    }

    @Override
    public Generator<Operation<?>> getLoadOperations( GeneratorBuilder generatorBuilder ) throws WorkloadException
    {
        /**
         * **************************
         * 
         * Insert Operation Generator
         * 
         * **************************
         */
        // Load Insert Keys
        Generator<Long> loadInsertKeyGenerator = generatorBuilder.counterGenerator( insertStart, 1l ).build();

        // Insert Fields: Names & Values
        Generator<Integer> fieldValuelengthGenerator = generatorBuilder.uniformNumberGenerator( 1, 100 ).build();
        Generator<ByteIterator> randomFieldValueGenerator = generatorBuilder.randomByteIteratorGenerator(
                fieldValuelengthGenerator ).build();
        Set<Triple<Double, String, Generator<ByteIterator>>> valuedFields = new HashSet<Triple<Double, String, Generator<ByteIterator>>>();
        for ( int i = 0; i < NUMBER_OF_FIELDS_IN_RECORD; i++ )
        {
            valuedFields.add( Triple.create( 1d, FIELD_NAME_PREFIX + i, randomFieldValueGenerator ) );
        }
        Generator<Map<String, ByteIterator>> insertValuedFieldGenerator = generatorBuilder.discreteValuedMultiGenerator(
                valuedFields, NUMBER_OF_FIELDS_IN_RECORD ).build();

        return new InsertOperationGenerator( TABLE,
                new PrefixGeneratorWrapper( loadInsertKeyGenerator, KEY_NAME_PREFIX ), insertValuedFieldGenerator );
    }

    @Override
    public Generator<Operation<?>> getTransactionalOperations( GeneratorBuilder generatorBuilder )
            throws WorkloadException
    {
        /**
         * **************************
         * 
         * Insert Operation Generator
         * 
         * **************************
         */
        // Transaction Insert Keys
        MinMaxGeneratorWrapper<Long> transactionInsertKeyGenerator = generatorBuilder.counterGenerator( recordCount, 1l ).withMinMax(
                recordCount, recordCount ).build();

        // Insert Fields: Names & Values
        Generator<Integer> fieldValuelengthGenerator = generatorBuilder.uniformNumberGenerator( 1, 100 ).build();
        Generator<ByteIterator> randomFieldValueGenerator = generatorBuilder.randomByteIteratorGenerator(
                fieldValuelengthGenerator ).build();
        Set<Triple<Double, String, Generator<ByteIterator>>> valuedFields = new HashSet<Triple<Double, String, Generator<ByteIterator>>>();
        for ( int i = 0; i < NUMBER_OF_FIELDS_IN_RECORD; i++ )
        {
            valuedFields.add( Triple.create( 1d, FIELD_NAME_PREFIX + i, randomFieldValueGenerator ) );
        }
        Generator<Map<String, ByteIterator>> insertValuedFieldGenerator = generatorBuilder.discreteValuedMultiGenerator(
                valuedFields, NUMBER_OF_FIELDS_IN_RECORD ).build();

        InsertOperationGenerator insertOperationGenerator = new InsertOperationGenerator( TABLE,
                new PrefixGeneratorWrapper( transactionInsertKeyGenerator, KEY_NAME_PREFIX ),
                insertValuedFieldGenerator );

        /**
         * **************************
         * 
         * Read Operation Generator
         * 
         * **************************
         */
        // Read/Update Keys
        Generator<String> requestKeyGenerator = generatorBuilder.dynamicRangeUniformNumberGenerator(
                transactionInsertKeyGenerator ).withPrefix( KEY_NAME_PREFIX ).build();

        // Read Fields: Names
        Set<Pair<Double, String>> fields = new HashSet<Pair<Double, String>>();
        for ( int i = 0; i < NUMBER_OF_FIELDS_IN_RECORD; i++ )
        {
            fields.add( Pair.create( 1d, FIELD_NAME_PREFIX + i ) );
        }

        Generator<Set<String>> readFieldsGenerator = generatorBuilder.discreteMultiGenerator( fields,
                NUMBER_OF_FIELDS_TO_READ ).build();

        ReadOperationGenerator readOperationGenerator = new ReadOperationGenerator( TABLE, requestKeyGenerator,
                readFieldsGenerator );

        /**
         * **************************
         * 
         * Update Operation Generator
         * 
         * **************************
         */
        // Update Fields: Names & Values
        Generator<Map<String, ByteIterator>> updateValuedFieldGenerator = generatorBuilder.discreteValuedMultiGenerator(
                valuedFields, NUMBER_OF_FIELDS_TO_UPDATE ).build();

        UpdateOperationGenerator updateOperationGenerator = new UpdateOperationGenerator( TABLE, requestKeyGenerator,
                updateValuedFieldGenerator );

        /**
         * **************************
         * 
         * Scan Operation Generator
         * 
         * **************************
         */
        // Scan Fields: Names & Values
        Generator<Set<String>> scanFieldGenerator = generatorBuilder.discreteMultiGenerator( fields,
                NUMBER_OF_FIELDS_TO_READ ).build();

        // Scan Length: Number of Records
        Generator<Integer> scanLengthGenerator = generatorBuilder.uniformNumberGenerator( MIN_SCAN_LENGTH,
                MAX_SCAN_LENGTH ).build();

        ScanOperationGenerator scanOperationGenerator = new ScanOperationGenerator( TABLE, requestKeyGenerator,
                scanLengthGenerator, scanFieldGenerator );

        /**
         * **************************
         * 
         * ReadModifyWrite Operation Generator
         * 
         * **************************
         */
        ReadModifyWriteOperationGenerator readModifyWriteOperationGenerator = new ReadModifyWriteOperationGenerator(
                TABLE, requestKeyGenerator, readFieldsGenerator, updateValuedFieldGenerator );

        /**
         * **************************
         * 
         * Transactional Workload Operations
         * 
         * **************************
         */
        // proportion of transactions reads/update/insert/scan/read-modify-write
        Set<Pair<Double, Generator<Operation<?>>>> operations = new HashSet<Pair<Double, Generator<Operation<?>>>>();
        operations.add( Pair.create( READ_RATIO, (Generator<Operation<?>>) readOperationGenerator ) );
        operations.add( Pair.create( UPDATE_RATIO, (Generator<Operation<?>>) updateOperationGenerator ) );
        operations.add( Pair.create( INSERT_RATIO, (Generator<Operation<?>>) insertOperationGenerator ) );
        operations.add( Pair.create( SCAN_RATIO, (Generator<Operation<?>>) scanOperationGenerator ) );
        operations.add( Pair.create( READ_MODIFY_WRITE_RATIO,
                (Generator<Operation<?>>) readModifyWriteOperationGenerator ) );
        return generatorBuilder.discreteValuedGenerator( operations ).build();
    }

    @Override
    protected void onCleanup() throws WorkloadException
    {
    }
}

class InsertOperationGenerator extends Generator<Operation<?>>
{
    private final String table;
    private final Generator<String> keyGenerator;
    private final Generator<Map<String, ByteIterator>> valuedFieldsGenerator;

    protected InsertOperationGenerator( String table, Generator<String> keyGenerator,
            Generator<Map<String, ByteIterator>> valuedFieldsGenerator )
    {
        super( null );
        this.table = table;
        this.keyGenerator = keyGenerator;
        this.valuedFieldsGenerator = valuedFieldsGenerator;
    }

    @Override
    protected Operation<?> doNext() throws GeneratorException
    {
        return new InsertOperation( table, keyGenerator.next(), valuedFieldsGenerator.next() );
    }
}

class ReadOperationGenerator extends Generator<Operation<?>>
{
    private final String table;
    private final Generator<String> keyGenerator;
    private final Generator<Set<String>> fieldsGenerator;

    protected ReadOperationGenerator( String table, Generator<String> keyGenerator,
            Generator<Set<String>> fieldsGenerator )
    {
        super( null );
        this.table = table;
        this.keyGenerator = keyGenerator;
        this.fieldsGenerator = fieldsGenerator;
    }

    @Override
    protected Operation<?> doNext() throws GeneratorException
    {
        return new ReadOperation( table, keyGenerator.next(), fieldsGenerator.next() );
    }
}

class UpdateOperationGenerator extends Generator<Operation<?>>
{
    private final String table;
    private final Generator<String> keyGenerator;
    private final Generator<Map<String, ByteIterator>> valuedFieldsGenerator;

    protected UpdateOperationGenerator( String table, Generator<String> keyGenerator,
            Generator<Map<String, ByteIterator>> valuedFieldsGenerator )
    {
        super( null );
        this.table = table;
        this.keyGenerator = keyGenerator;
        this.valuedFieldsGenerator = valuedFieldsGenerator;
    }

    @Override
    protected Operation<?> doNext() throws GeneratorException
    {
        return new UpdateOperation( table, keyGenerator.next(), valuedFieldsGenerator.next() );
    }
}

class ScanOperationGenerator extends Generator<Operation<?>>
{
    private final String table;
    private final Generator<String> startKeyGenerator;
    private final Generator<Integer> recordCountGenerator;
    private final Generator<Set<String>> fieldsGenerator;

    protected ScanOperationGenerator( String table, Generator<String> startKeyGenerator,
            Generator<Integer> recordCountGenerator, Generator<Set<String>> fieldsGenerator )
    {
        super( null );
        this.table = table;
        this.startKeyGenerator = startKeyGenerator;
        this.recordCountGenerator = recordCountGenerator;
        this.fieldsGenerator = fieldsGenerator;
    }

    @Override
    protected Operation<?> doNext() throws GeneratorException
    {
        return new ScanOperation( table, startKeyGenerator.next(), recordCountGenerator.next(), fieldsGenerator.next() );
    }
}

class ReadModifyWriteOperationGenerator extends Generator<Operation<?>>
{
    private final String table;
    private final Generator<String> keyGenerator;
    private final Generator<Set<String>> fieldsGenerator;
    private final Generator<Map<String, ByteIterator>> valuedFieldsGenerator;

    protected ReadModifyWriteOperationGenerator( String table, Generator<String> keyGenerator,
            Generator<Set<String>> fieldsGenerator, Generator<Map<String, ByteIterator>> valuedFieldsGenerator )
    {
        super( null );
        this.table = table;
        this.keyGenerator = keyGenerator;
        this.fieldsGenerator = fieldsGenerator;
        this.valuedFieldsGenerator = valuedFieldsGenerator;
    }

    @Override
    protected Operation<?> doNext() throws GeneratorException
    {
        return new ReadModifyWriteOperation( table, keyGenerator.next(), fieldsGenerator.next(),
                valuedFieldsGenerator.next() );
    }
}
