package com.ldbc.driver.workloads.simple;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.data.ByteIterator;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.MinMaxGenerator;
import com.ldbc.driver.generator.PrefixGenerator;
import com.ldbc.driver.generator.StartTimeAssigningOperationGenerator;
import com.ldbc.driver.util.GeneratorUtils;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple.Tuple2;
import com.ldbc.driver.util.Tuple.Tuple3;
import com.ldbc.driver.util.temporal.Duration;
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
    public Iterator<Operation<?>> createLoadOperations( GeneratorFactory generators ) throws WorkloadException
    {
        /**
         * **************************
         * 
         * Insert Operation Generator
         * 
         * **************************
         */
        // Load Insert Keys
        Iterator<Long> loadInsertKeyGenerator = generators.incrementing( 0l, 1l );

        // Insert Fields: Names & Values
        Iterator<Integer> fieldValuelengthGenerator = generators.uniform( 1, 100 );
        Iterator<ByteIterator> randomFieldValueGenerator = generators.randomByteIterator( fieldValuelengthGenerator );
        Set<Tuple3<Double, String, Iterator<ByteIterator>>> valuedFields = new HashSet<Tuple3<Double, String, Iterator<ByteIterator>>>();
        for ( int i = 0; i < NUMBER_OF_FIELDS_IN_RECORD; i++ )
        {
            valuedFields.add( Tuple.tuple3( 1d, FIELD_NAME_PREFIX + i, randomFieldValueGenerator ) );
        }
        Iterator<Map<String, ByteIterator>> insertValuedFieldGenerator = generators.weightedDiscreteMap( valuedFields,
                NUMBER_OF_FIELDS_IN_RECORD );

        Iterator<Operation<?>> insertOperationGenerator = new InsertOperationGenerator( TABLE, new PrefixGenerator(
                loadInsertKeyGenerator, KEY_NAME_PREFIX ), insertValuedFieldGenerator );

        Iterator<Time> startTimeGenerator = GeneratorUtils.randomIncrementStartTimeGenerator( generators, Time.now(),
                Duration.fromMilli( 100 ), Duration.fromMilli( 1000 ) );

        return new StartTimeAssigningOperationGenerator( startTimeGenerator, insertOperationGenerator );
    }

    @Override
    public Iterator<Operation<?>> createTransactionalOperations( GeneratorFactory generators ) throws WorkloadException
    {
        /**
         * **************************
         * 
         * Insert Operation Generator
         * 
         * **************************
         */
        // Transaction Insert Keys
        MinMaxGenerator<Long> transactionInsertKeyGenerator = generators.minMaxGenerator(
                generators.incrementing( getRecordCount(), 1l ), getRecordCount(), getRecordCount() );

        // Insert Fields: Names & Values
        Iterator<Integer> fieldValuelengthGenerator = generators.uniform( 1, 100 );
        Iterator<ByteIterator> randomFieldValueGenerator = generators.randomByteIterator( fieldValuelengthGenerator );
        Set<Tuple3<Double, String, Iterator<ByteIterator>>> valuedFields = new HashSet<Tuple3<Double, String, Iterator<ByteIterator>>>();
        for ( int i = 0; i < NUMBER_OF_FIELDS_IN_RECORD; i++ )
        {
            valuedFields.add( Tuple.tuple3( 1d, FIELD_NAME_PREFIX + i, randomFieldValueGenerator ) );
        }
        Iterator<Map<String, ByteIterator>> insertValuedFieldGenerator = generators.weightedDiscreteMap( valuedFields,
                NUMBER_OF_FIELDS_IN_RECORD );

        InsertOperationGenerator insertOperationGenerator = new InsertOperationGenerator( TABLE, new PrefixGenerator(
                transactionInsertKeyGenerator, KEY_NAME_PREFIX ), insertValuedFieldGenerator );

        /**
         * **************************
         * 
         * Read Operation Generator
         * 
         * **************************
         */
        // Read/Update Keys
        Iterator<String> requestKeyGenerator = generators.prefix(
                generators.dynamicRangeUniform( transactionInsertKeyGenerator ), KEY_NAME_PREFIX );

        // Read Fields: Names
        Set<Tuple2<Double, String>> fields = new HashSet<Tuple2<Double, String>>();
        for ( int i = 0; i < NUMBER_OF_FIELDS_IN_RECORD; i++ )
        {
            fields.add( Tuple.tuple2( 1d, FIELD_NAME_PREFIX + i ) );
        }

        Iterator<Set<String>> readFieldsGenerator = generators.weightedDiscreteSet( fields, NUMBER_OF_FIELDS_TO_READ );

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
        Iterator<Map<String, ByteIterator>> updateValuedFieldsGenerator = generators.weightedDiscreteMap( valuedFields,
                NUMBER_OF_FIELDS_TO_UPDATE );

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
        Iterator<Set<String>> scanFieldsGenerator = generators.weightedDiscreteSet( fields, NUMBER_OF_FIELDS_TO_READ );

        // Scan Length: Number of Records
        Iterator<Integer> scanLengthGenerator = generators.uniform( MIN_SCAN_LENGTH, MAX_SCAN_LENGTH );

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
        Set<Tuple2<Double, Iterator<Operation<?>>>> operations = new HashSet<Tuple2<Double, Iterator<Operation<?>>>>();
        operations.add( Tuple.tuple2( READ_RATIO, (Iterator<Operation<?>>) readOperationGenerator ) );
        operations.add( Tuple.tuple2( UPDATE_RATIO, (Iterator<Operation<?>>) updateOperationGenerator ) );
        operations.add( Tuple.tuple2( INSERT_RATIO, (Iterator<Operation<?>>) insertOperationGenerator ) );
        operations.add( Tuple.tuple2( SCAN_RATIO, (Iterator<Operation<?>>) scanOperationGenerator ) );
        operations.add( Tuple.tuple2( READ_MODIFY_WRITE_RATIO,
                (Iterator<Operation<?>>) readModifyWriteOperationGenerator ) );

        Iterator<Operation<?>> transactionalOperationGenerator = generators.weightedDiscreteDereferencing( operations );

        Iterator<Time> startTimeGenerator = GeneratorUtils.constantIncrementStartTimeGenerator( generators, Time.now(),
                Duration.fromMilli( 100 ) );

        return new StartTimeAssigningOperationGenerator( startTimeGenerator, transactionalOperationGenerator );
    }

    @Override
    protected void onCleanup() throws WorkloadException
    {
    }
}

class InsertOperationGenerator extends Generator<Operation<?>>
{
    private final String table;
    private final Iterator<String> keyGenerator;
    private final Iterator<Map<String, ByteIterator>> valuedFieldsGenerator;

    protected InsertOperationGenerator( String table, Iterator<String> keyGenerator,
            Iterator<Map<String, ByteIterator>> valuedFieldsGenerator )
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
    private final Iterator<String> keyGenerator;
    private final Iterator<Set<String>> fieldsGenerator;

    protected ReadOperationGenerator( String table, Iterator<String> keyGenerator, Iterator<Set<String>> fieldsGenerator )
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
    private final Iterator<String> keyGenerator;
    private final Iterator<Map<String, ByteIterator>> valuedFieldsGenerator;

    protected UpdateOperationGenerator( String table, Iterator<String> keyGenerator,
            Iterator<Map<String, ByteIterator>> valuedFieldsGenerator )
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
    private final Iterator<String> startKeyGenerator;
    private final Iterator<Integer> recordCountGenerator;
    private final Iterator<Set<String>> fieldsGenerator;

    protected ScanOperationGenerator( String table, Iterator<String> startKeyGenerator,
            Iterator<Integer> recordCountGenerator, Iterator<Set<String>> fieldsGenerator )
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
    private final Iterator<String> keyGenerator;
    private final Iterator<Set<String>> fieldsGenerator;
    private final Iterator<Map<String, ByteIterator>> valuedFieldsGenerator;

    protected ReadModifyWriteOperationGenerator( String table, Iterator<String> keyGenerator,
            Iterator<Set<String>> fieldsGenerator, Iterator<Map<String, ByteIterator>> valuedFieldsGenerator )
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
