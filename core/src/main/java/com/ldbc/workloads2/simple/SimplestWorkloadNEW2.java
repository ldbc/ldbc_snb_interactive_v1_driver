package com.ldbc.workloads2.simple;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.ldbc.Client;
import com.ldbc.data.ByteIterator;
import com.ldbc.db2.Operation2;
import com.ldbc.generator.Generator;
import com.ldbc.generator.GeneratorBuilder;
import com.ldbc.generator.GeneratorException;
import com.ldbc.generator.MinMaxGeneratorWrapper;
import com.ldbc.generator.PrefixGeneratorWrapper;
import com.ldbc.util.Pair;
import com.ldbc.util.MapUtils;
import com.ldbc.util.Triple;
import com.ldbc.workloads2.Workload2;
import com.ldbc.workloads2.WorkloadException2;
import com.ldbc2.Client2;

public class SimplestWorkloadNEW2 extends Workload2
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

    final long insertStart;
    final long recordCount;

    public SimplestWorkloadNEW2( Map<String, String> properties ) throws WorkloadException2
    {
        super( properties );
        recordCount = Long.parseLong( properties.get( Client.RECORD_COUNT ) );
        insertStart = Long.parseLong( MapUtils.mapGetDefault( properties, Client2.INSERT_START,
                Client2.INSERT_START_DEFAULT ) );
    }

    @Override
    public Generator<Operation2<?>> getLoadOperations( GeneratorBuilder generatorBuilder ) throws WorkloadException2
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
    public Generator<Operation2<?>> getTransactionalOperations( GeneratorBuilder generatorBuilder )
            throws WorkloadException2
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
        Set<Pair<Double, Generator<Operation2<?>>>> operations = new HashSet<Pair<Double, Generator<Operation2<?>>>>();
        operations.add( Pair.create( READ_RATIO, (Generator<Operation2<?>>) readOperationGenerator ) );
        operations.add( Pair.create( UPDATE_RATIO, (Generator<Operation2<?>>) updateOperationGenerator ) );
        operations.add( Pair.create( INSERT_RATIO, (Generator<Operation2<?>>) insertOperationGenerator ) );
        operations.add( Pair.create( SCAN_RATIO, (Generator<Operation2<?>>) scanOperationGenerator ) );
        operations.add( Pair.create( READ_MODIFY_WRITE_RATIO,
                (Generator<Operation2<?>>) readModifyWriteOperationGenerator ) );
        return generatorBuilder.discreteValuedGenerator( operations ).build();
    }
}

class InsertOperationGenerator extends Generator<Operation2<?>>
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
    protected Operation2<?> doNext() throws GeneratorException
    {
        return new InsertOperation2( table, keyGenerator.next(), valuedFieldsGenerator.next() );
    }
}

class ReadOperationGenerator extends Generator<Operation2<?>>
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
    protected Operation2<?> doNext() throws GeneratorException
    {
        return new ReadOperation2( table, keyGenerator.next(), fieldsGenerator.next() );
    }
}

class UpdateOperationGenerator extends Generator<Operation2<?>>
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
    protected Operation2<?> doNext() throws GeneratorException
    {
        return new UpdateOperation2( table, keyGenerator.next(), valuedFieldsGenerator.next() );
    }
}

class ScanOperationGenerator extends Generator<Operation2<?>>
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
    protected Operation2<?> doNext() throws GeneratorException
    {
        return new ScanOperation2( table, startKeyGenerator.next(), recordCountGenerator.next(), fieldsGenerator.next() );
    }
}

class ReadModifyWriteOperationGenerator extends Generator<Operation2<?>>
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
    protected Operation2<?> doNext() throws GeneratorException
    {
        return new ReadModifyWriteOperation2( table, keyGenerator.next(), fieldsGenerator.next(),
                valuedFieldsGenerator.next() );
    }
}
