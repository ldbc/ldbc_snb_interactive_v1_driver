package com.ldbc.driver.workloads.simple;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.data.ByteIterator;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.wrapper.MinMaxGeneratorWrapper;
import com.ldbc.driver.generator.wrapper.PrefixGeneratorWrapper;
import com.ldbc.driver.generator.wrapper.StartTimeOperationGeneratorWrapper;
import com.ldbc.driver.util.GeneratorUtils;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple.Tuple2;
import com.ldbc.driver.util.Tuple.Tuple3;
import com.ldbc.driver.util.temporal.Time;

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

    final double READ_RATIO = 0.20;
    final double UPDATE_RATIO = 0.20;
    final double INSERT_RATIO = 0.20;
    final double SCAN_RATIO = 0.20;
    final double READ_MODIFY_WRITE_RATIO = 0.20;

    @Override
    public void onInit( Map<String, String> properties )
    {
    }

    @Override
    public Generator<Operation<?>> createLoadOperations( GeneratorFactory generatorBuilder ) throws WorkloadException
    {
        /**
         * **************************
         * 
         * Insert Operation Generator
         * 
         * **************************
         */
        // Load Insert Keys
        Generator<Long> loadInsertKeyGenerator = generatorBuilder.incrementingGenerator( 0l, 1l );

        // Insert Fields: Names & Values
        Generator<Integer> fieldValuelengthGenerator = generatorBuilder.uniformNumberGenerator( 1, 100 );
        Generator<ByteIterator> randomFieldValueGenerator = generatorBuilder.randomByteIteratorGenerator( fieldValuelengthGenerator );
        Set<Tuple3<Double, String, Generator<ByteIterator>>> valuedFields = new HashSet<Tuple3<Double, String, Generator<ByteIterator>>>();
        for ( int i = 0; i < NUMBER_OF_FIELDS_IN_RECORD; i++ )
        {
            valuedFields.add( Tuple.tuple3( 1d, FIELD_NAME_PREFIX + i, randomFieldValueGenerator ) );
        }
        Generator<Map<String, ByteIterator>> insertValuedFieldGenerator = generatorBuilder.weightedDiscreteMapGenerator(
                valuedFields, NUMBER_OF_FIELDS_IN_RECORD );

        Generator<Operation<?>> insertOperationGenerator = new InsertOperationGenerator( TABLE,
                new PrefixGeneratorWrapper( loadInsertKeyGenerator, KEY_NAME_PREFIX ), insertValuedFieldGenerator );

        Generator<Time> startTimeGenerator = GeneratorUtils.randomTimeGeneratorFromNow( generatorBuilder );

        return new StartTimeOperationGeneratorWrapper( startTimeGenerator, insertOperationGenerator );
    }

    @Override
    public Generator<Operation<?>> createTransactionalOperations( GeneratorFactory generators )
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
        MinMaxGeneratorWrapper<Long> transactionInsertKeyGenerator = generators.minMaxGeneratorWrapper(
                generators.incrementingGenerator( getRecordCount(), 1l ), getRecordCount(), getRecordCount() );

        // Insert Fields: Names & Values
        Generator<Integer> fieldValuelengthGenerator = generators.uniformNumberGenerator( 1, 100 );
        Generator<ByteIterator> randomFieldValueGenerator = generators.randomByteIteratorGenerator( fieldValuelengthGenerator );
        Set<Tuple3<Double, String, Generator<ByteIterator>>> valuedFields = new HashSet<Tuple3<Double, String, Generator<ByteIterator>>>();
        for ( int i = 0; i < NUMBER_OF_FIELDS_IN_RECORD; i++ )
        {
            valuedFields.add( Tuple.tuple3( 1d, FIELD_NAME_PREFIX + i, randomFieldValueGenerator ) );
        }
        Generator<Map<String, ByteIterator>> insertValuedFieldGenerator = generators.weightedDiscreteMapGenerator(
                valuedFields, NUMBER_OF_FIELDS_IN_RECORD );

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
        Generator<String> requestKeyGenerator = generators.prefixGeneratorWrapper(
                generators.dynamicRangeUniformNumberGenerator( transactionInsertKeyGenerator ), KEY_NAME_PREFIX );

        // Read Fields: Names
        Set<Tuple2<Double, String>> fields = new HashSet<Tuple2<Double, String>>();
        for ( int i = 0; i < NUMBER_OF_FIELDS_IN_RECORD; i++ )
        {
            fields.add( Tuple.tuple2( 1d, FIELD_NAME_PREFIX + i ) );
        }

        Generator<Set<String>> readFieldsGenerator = generators.weightedDiscreteSetGenerator( fields,
                NUMBER_OF_FIELDS_TO_READ );

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
        Generator<Map<String, ByteIterator>> updateValuedFieldsGenerator = generators.weightedDiscreteMapGenerator(
                valuedFields, NUMBER_OF_FIELDS_TO_UPDATE );

        UpdateOperationGenerator updateOperationGenerator = new UpdateOperationGenerator( TABLE, requestKeyGenerator,
                updateValuedFieldsGenerator );

        /**
         * **************************
         * 
         * Scan Operation Generator
         * 
         * **************************
         */
        // Scan Fields: Names & Values
        Generator<Set<String>> scanFieldsGenerator = generators.weightedDiscreteSetGenerator( fields,
                NUMBER_OF_FIELDS_TO_READ );

        // Scan Length: Number of Records
        Generator<Integer> scanLengthGenerator = generators.uniformNumberGenerator( MIN_SCAN_LENGTH, MAX_SCAN_LENGTH );

        ScanOperationGenerator scanOperationGenerator = new ScanOperationGenerator( TABLE, requestKeyGenerator,
                scanLengthGenerator, scanFieldsGenerator );

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
        Set<Tuple2<Double, Generator<Operation<?>>>> operations = new HashSet<Tuple2<Double, Generator<Operation<?>>>>();
        operations.add( Tuple.tuple2( READ_RATIO, (Generator<Operation<?>>) readOperationGenerator ) );
        operations.add( Tuple.tuple2( UPDATE_RATIO, (Generator<Operation<?>>) updateOperationGenerator ) );
        operations.add( Tuple.tuple2( INSERT_RATIO, (Generator<Operation<?>>) insertOperationGenerator ) );
        operations.add( Tuple.tuple2( SCAN_RATIO, (Generator<Operation<?>>) scanOperationGenerator ) );
        operations.add( Tuple.tuple2( READ_MODIFY_WRITE_RATIO,
                (Generator<Operation<?>>) readModifyWriteOperationGenerator ) );

        Generator<Operation<?>> transactionalOperationGenerator = generators.weightedDiscreteDereferencingGenerator( operations );

        Generator<Time> startTimeGenerator = GeneratorUtils.randomTimeGeneratorFromNow( generators );

        return new StartTimeOperationGeneratorWrapper( startTimeGenerator, transactionalOperationGenerator );
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
