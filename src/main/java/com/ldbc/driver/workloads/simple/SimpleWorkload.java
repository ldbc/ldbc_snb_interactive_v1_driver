package com.ldbc.driver.workloads.simple;

import com.google.common.collect.Iterators;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationClassification;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.MinMaxGenerator;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple.Tuple2;
import com.ldbc.driver.util.Tuple.Tuple3;

import java.util.*;

public class SimpleWorkload extends Workload {
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
    public Map<Class<? extends Operation>, OperationClassification> getOperationClassifications() {
        Map<Class<? extends Operation>, OperationClassification> operationClassificationMapping = new HashMap<>();
        // TODO use correct operation classifications
        operationClassificationMapping.put(InsertOperation.class, new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
        operationClassificationMapping.put(ReadModifyWriteOperation.class, new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
        operationClassificationMapping.put(ReadOperation.class, new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
        operationClassificationMapping.put(ScanOperation.class, new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
        operationClassificationMapping.put(UpdateOperation.class, new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
        return operationClassificationMapping;
    }

    @Override
    public void onInit(Map<String, String> params) {
    }

    @Override
    public Iterator<Operation<?>> getOperations(GeneratorFactory gf) throws WorkloadException {
        Time workloadStartTime = Time.fromMilli(0);

        /**
         * **************************
         *
         * Initial Insert Operation Generator
         *
         * **************************
         */
        // Load Insert Keys
        MinMaxGenerator<Long> insertKeyGenerator = gf.minMaxGenerator(gf.incrementing(0l, 1l), 0l, 0l);

        // Insert Fields: Names & Values
        Iterator<Long> fieldValueLengthGenerator = gf.uniform(1l, 100l);
        Iterator<Iterator<Byte>> randomFieldValueGenerator = gf.sizedUniformBytesGenerator(fieldValueLengthGenerator);
        List<Tuple3<Double, String, Iterator<Iterator<Byte>>>> valuedFields = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_FIELDS_IN_RECORD; i++) {
            valuedFields.add(Tuple.tuple3(1d, FIELD_NAME_PREFIX + i, randomFieldValueGenerator));
        }
        Iterator<Map<String, Iterator<Byte>>> insertValuedFieldGenerator = gf.weightedDiscreteMap(valuedFields, NUMBER_OF_FIELDS_IN_RECORD);

        Iterator<Operation<?>> initialInsertOperationGenerator = gf.limit(
                new InsertOperationGenerator(TABLE, gf.prefix(insertKeyGenerator, KEY_NAME_PREFIX), insertValuedFieldGenerator),
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
                gf.prefix(insertKeyGenerator, KEY_NAME_PREFIX),
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
        Iterator<String> requestKeyGenerator = gf.prefix(gf.dynamicRangeUniform(insertKeyGenerator), KEY_NAME_PREFIX);

        // Read Fields: Names
        List<Tuple2<Double, String>> fields = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_FIELDS_IN_RECORD; i++) {
            fields.add(Tuple.tuple2(1d, FIELD_NAME_PREFIX + i));
        }

        Iterator<List<String>> readFieldsGenerator = gf.weightedDiscreteList(fields, NUMBER_OF_FIELDS_TO_READ);

        ReadOperationGenerator readOperationGenerator = new ReadOperationGenerator(
                TABLE,
                requestKeyGenerator,
                readFieldsGenerator);

        /**
         * **************************
         *
         * Update Operation Generator
         *
         * **************************
         */
        // Update Fields: Names & Values
        Iterator<Map<String, Iterator<Byte>>> updateValuedFieldsGenerator = gf.weightedDiscreteMap(
                valuedFields,
                NUMBER_OF_FIELDS_TO_UPDATE);

        UpdateOperationGenerator updateOperationGenerator = new UpdateOperationGenerator(
                TABLE,
                requestKeyGenerator,
                updateValuedFieldsGenerator);

        /**
         * **************************
         *
         * Scan Operation Generator
         *
         * **************************
         */
        // Scan Fields: Names & Values
        Iterator<List<String>> scanFieldsGenerator = gf.weightedDiscreteList(fields, NUMBER_OF_FIELDS_TO_READ);

        // Scan Length: Number of Records
        Iterator<Integer> scanLengthGenerator = gf.uniform(MIN_SCAN_LENGTH, MAX_SCAN_LENGTH);

        ScanOperationGenerator scanOperationGenerator = new ScanOperationGenerator(
                TABLE,
                requestKeyGenerator,
                scanLengthGenerator,
                scanFieldsGenerator);

        /**
         * **************************
         *
         * ReadModifyWrite Operation Generator
         *
         * **************************
         */
        ReadModifyWriteOperationGenerator readModifyWriteOperationGenerator = new ReadModifyWriteOperationGenerator(
                TABLE, requestKeyGenerator, readFieldsGenerator, updateValuedFieldsGenerator);

        /**
         * **************************
         *
         * Transactional Workload Operations
         *
         * **************************
         */
        // proportion of transactions reads/update/insert/scan/read-modify-write
        List<Tuple2<Double, Iterator<Operation<?>>>> operations = new ArrayList<>();
        operations.add(Tuple.tuple2(READ_RATIO, (Iterator<Operation<?>>) readOperationGenerator));
        operations.add(Tuple.tuple2(UPDATE_RATIO, (Iterator<Operation<?>>) updateOperationGenerator));
        operations.add(Tuple.tuple2(INSERT_RATIO, (Iterator<Operation<?>>) transactionalInsertOperationGenerator));
        operations.add(Tuple.tuple2(SCAN_RATIO, (Iterator<Operation<?>>) scanOperationGenerator));
        operations.add(Tuple.tuple2(READ_MODIFY_WRITE_RATIO, (Iterator<Operation<?>>) readModifyWriteOperationGenerator));

        Iterator<Operation<?>> transactionalOperationGenerator = gf.weightedDiscreteDereferencing(operations);

        // iterates initialInsertOperationGenerator before starting with transactionalInsertOperationGenerator
        Iterator<Operation<?>> workloadOperations = Iterators.concat(initialInsertOperationGenerator, transactionalOperationGenerator);

        Iterator<Time> startTimes = gf.constantIncrementTime(workloadStartTime.plus(Duration.fromMilli(1)), Duration.fromMilli(100));
        Iterator<Time> dependencyTimes = gf.constant(workloadStartTime);

        return gf.assignDependencyTimes(dependencyTimes, gf.assignStartTimes(startTimes, workloadOperations));
    }

    @Override
    protected void onCleanup() throws WorkloadException {
    }

    @Override
    public String serializeOperation(Operation<?> instance) {
        return null;
    }

    @Override
    public Operation<?> marshalOperation(String serializedInstance) {
        return null;
    }
}

