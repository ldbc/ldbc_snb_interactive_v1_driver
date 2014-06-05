package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationClassification;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.StartTimeAssigningOperationGenerator;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.*;
import com.ldbc.driver.util.Tuple.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

import static com.ldbc.driver.OperationClassification.GctMode;
import static com.ldbc.driver.OperationClassification.SchedulingMode;
import static com.ldbc.driver.generator.CsvEventStreamReader.EventReturnPolicy;

public class LdbcInteractiveWorkload extends Workload {
    public final static String DATA_DIRECTORY = "data_dir";
    public final static String PARAMETERS_DIRECTORY = "parameters_dir";
    public final static String INTERLEAVE_DURATION_KEY = "interleave_duration";
    public final static String READ_RATIO_KEY = "read_ratio";
    public final static String WRITE_RATIO_KEY = "write_ratio";

    public final static String READ_OPERATION_1_KEY = LdbcQuery1.class.getName();
    public final static String READ_OPERATION_2_KEY = LdbcQuery2.class.getName();
    public final static String READ_OPERATION_3_KEY = LdbcQuery3.class.getName();
    public final static String READ_OPERATION_4_KEY = LdbcQuery4.class.getName();
    public final static String READ_OPERATION_5_KEY = LdbcQuery5.class.getName();
    public final static String READ_OPERATION_6_KEY = LdbcQuery6.class.getName();
    public final static String READ_OPERATION_7_KEY = LdbcQuery7.class.getName();
    public final static String READ_OPERATION_8_KEY = LdbcQuery8.class.getName();
    public final static String READ_OPERATION_9_KEY = LdbcQuery9.class.getName();
    public final static String READ_OPERATION_10_KEY = LdbcQuery10.class.getName();
    public final static String READ_OPERATION_11_KEY = LdbcQuery11.class.getName();
    public final static String READ_OPERATION_12_KEY = LdbcQuery12.class.getName();
    public final static String READ_OPERATION_13_KEY = LdbcQuery13.class.getName();
    public final static String READ_OPERATION_14_KEY = LdbcQuery14.class.getName();
    public final static List<String> READ_OPERATION_KEYS = Lists.newArrayList(
            READ_OPERATION_1_KEY,
            READ_OPERATION_2_KEY,
            READ_OPERATION_3_KEY,
            READ_OPERATION_4_KEY,
            READ_OPERATION_5_KEY,
            READ_OPERATION_6_KEY,
            READ_OPERATION_7_KEY,
            READ_OPERATION_8_KEY,
            READ_OPERATION_9_KEY,
            READ_OPERATION_10_KEY,
            READ_OPERATION_11_KEY,
            READ_OPERATION_12_KEY,
            READ_OPERATION_13_KEY,
            READ_OPERATION_14_KEY);

    public final static String WRITE_OPERATION_1_KEY = LdbcUpdate1AddPerson.class.getName();
    public final static String WRITE_OPERATION_2_KEY = LdbcUpdate2AddPostLike.class.getName();
    public final static String WRITE_OPERATION_3_KEY = LdbcUpdate3AddCommentLike.class.getName();
    public final static String WRITE_OPERATION_4_KEY = LdbcUpdate4AddForum.class.getName();
    public final static String WRITE_OPERATION_5_KEY = LdbcUpdate5AddForumMembership.class.getName();
    public final static String WRITE_OPERATION_6_KEY = LdbcUpdate6AddPost.class.getName();
    public final static String WRITE_OPERATION_7_KEY = LdbcUpdate7AddComment.class.getName();
    public final static String WRITE_OPERATION_8_KEY = LdbcUpdate8AddFriendship.class.getName();
    public final static List<String> WRITE_OPERATION_KEYS = Lists.newArrayList(
            WRITE_OPERATION_1_KEY,
            WRITE_OPERATION_2_KEY,
            WRITE_OPERATION_3_KEY,
            WRITE_OPERATION_4_KEY,
            WRITE_OPERATION_5_KEY,
            WRITE_OPERATION_6_KEY,
            WRITE_OPERATION_7_KEY,
            WRITE_OPERATION_8_KEY
    );

    public final static String READ_OPERATION_1_PARAMS_FILENAME = "query_1_param.txt";
    public final static String READ_OPERATION_2_PARAMS_FILENAME = "query_2_param.txt";
    public final static String READ_OPERATION_3_PARAMS_FILENAME = "query_3_param.txt";
    public final static String READ_OPERATION_4_PARAMS_FILENAME = "query_4_param.txt";
    public final static String READ_OPERATION_5_PARAMS_FILENAME = "query_5_param.txt";
    public final static String READ_OPERATION_6_PARAMS_FILENAME = "query_6_param.txt";
    public final static String READ_OPERATION_7_PARAMS_FILENAME = "query_7_param.txt";
    public final static String READ_OPERATION_8_PARAMS_FILENAME = "query_8_param.txt";
    public final static String READ_OPERATION_9_PARAMS_FILENAME = "query_9_param.txt";
    public final static String READ_OPERATION_10_PARAMS_FILENAME = "query_10_param.txt";
    public final static String READ_OPERATION_11_PARAMS_FILENAME = "query_11_param.txt";
    public final static String READ_OPERATION_12_PARAMS_FILENAME = "query_12_param.txt";
    public final static String READ_OPERATION_13_PARAMS_FILENAME = "query_13_param.txt";
    public final static String READ_OPERATION_14_PARAMS_FILENAME = "query_14_param.txt";
    public final static List<String> READ_OPERATION_PARAMS_FILENAMES = Lists.newArrayList(
            READ_OPERATION_1_PARAMS_FILENAME,
            READ_OPERATION_2_PARAMS_FILENAME,
            READ_OPERATION_3_PARAMS_FILENAME,
            READ_OPERATION_4_PARAMS_FILENAME,
            READ_OPERATION_5_PARAMS_FILENAME,
            READ_OPERATION_6_PARAMS_FILENAME,
            READ_OPERATION_7_PARAMS_FILENAME,
            READ_OPERATION_8_PARAMS_FILENAME,
            READ_OPERATION_9_PARAMS_FILENAME,
            READ_OPERATION_10_PARAMS_FILENAME,
            READ_OPERATION_11_PARAMS_FILENAME,
            READ_OPERATION_12_PARAMS_FILENAME,
            READ_OPERATION_13_PARAMS_FILENAME,
            READ_OPERATION_14_PARAMS_FILENAME
    );

    public final static String WRITE_EVENTS_FILENAME = "updateStream_0.csv";

    private WriteEventStreamReader writeOperationsReader;

    private final static String CSV_SEPARATOR = "\\|";
    private CsvFileReader readOperation1FileReader;
    private CsvFileReader readOperation2FileReader;
    private CsvFileReader readOperation3FileReader;
    private CsvFileReader readOperation4FileReader;
    private CsvFileReader readOperation5FileReader;
    private CsvFileReader readOperation6FileReader;
    private CsvFileReader readOperation7FileReader;
    private CsvFileReader readOperation8FileReader;
    private CsvFileReader readOperation9FileReader;
    private CsvFileReader readOperation10FileReader;
    private CsvFileReader readOperation11FileReader;
    private CsvFileReader readOperation12FileReader;
    private CsvFileReader readOperation13FileReader;
    private CsvFileReader readOperation14FileReader;
    private Duration interleaveDuration;
    private double readRatio;
    private double writeRatio;
    private Map<Class, Double> readOperationRatios;
    private Set<Class> writeOperationFilter;

    @Override
    public Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications() {
        Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications = new HashMap<Class<? extends Operation<?>>, OperationClassification>();
        /*
         * TODO assign correct Modes
         * Modes (with examples from LDBC Interactive SNB Workload):
         * - WINDOWED & NONE -------------------> n/a
         * - WINDOWED & READ -------------------> Create Friendship
         * - WINDOWED & READ WRITE -------------> Create User
         * - INDIVIDUAL_BLOCKING & NONE --------> n/a
         * - INDIVIDUAL_BLOCKING & READ --------> Create Post
         * - INDIVIDUAL_BLOCKING & READ WRITE --> n/a
         * - INDIVIDUAL_ASYNC & NONE -----------> Entire Read Workload
         * - INDIVIDUAL_ASYNC & READ -----------> n/a
         * - INDIVIDUAL_ASYNC & READ WRITE -----> n/a
        */
        operationClassifications.put(LdbcQuery1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery3.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery4.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery5.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery6.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery7.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery8.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery9.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery10.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery11.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery12.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery13.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery14.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcUpdate1AddPerson.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcUpdate2AddPostLike.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcUpdate3AddCommentLike.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcUpdate4AddForum.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcUpdate5AddForumMembership.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcUpdate6AddPost.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcUpdate7AddComment.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcUpdate8AddFriendship.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        return operationClassifications;
    }

    @Override
    public void onInit(Map<String, String> properties) throws WorkloadException {
        List<String> compulsoryKeys = Lists.newArrayList(
                DATA_DIRECTORY,
                PARAMETERS_DIRECTORY,
                INTERLEAVE_DURATION_KEY,
                READ_RATIO_KEY,
                WRITE_RATIO_KEY);
        compulsoryKeys.addAll(READ_OPERATION_KEYS);
        compulsoryKeys.addAll(WRITE_OPERATION_KEYS);

        List<String> missingPropertyParameters = missingPropertiesParameters(properties, compulsoryKeys);
        if (false == missingPropertyParameters.isEmpty())
            throw new WorkloadException(String.format("Workload could not initialize due to missing parameters: %s", missingPropertyParameters.toString()));

        File dataDir = new File(properties.get(DATA_DIRECTORY));
        if (false == dataDir.exists()) {
            throw new WorkloadException(String.format("Data directory does not exist: %s", dataDir.getAbsolutePath()));
        }
        File writeEventsFile = new File(dataDir.getAbsolutePath() + "/" + WRITE_EVENTS_FILENAME);
        if (false == writeEventsFile.exists()) {
            throw new WorkloadException(String.format("Write events file does not exist: %s", writeEventsFile.getAbsolutePath()));
        }
        try {
            writeOperationsReader = new WriteEventStreamReader(writeEventsFile);
        } catch (FileNotFoundException e) {
            throw new WorkloadException("Unable to load write operation parameters file", e);
        }

        File parametersDir = new File(properties.get(PARAMETERS_DIRECTORY));
        if (false == parametersDir.exists()) {
            throw new WorkloadException(String.format("Parameters directory does not exist: %s", parametersDir.getAbsolutePath()));
        }
        for (String readOperationParamsFilename : READ_OPERATION_PARAMS_FILENAMES) {
            String readOperationParamsFullPath = parametersDir.getAbsolutePath() + "/" + readOperationParamsFilename;
            if (false == new File(readOperationParamsFullPath).exists()) {
                throw new WorkloadException(String.format("Read operation parameters file does not exist: %s", readOperationParamsFullPath));
            }
        }
        try {
            readOperation1FileReader = new CsvFileReader(new File(parametersDir.getAbsolutePath() + "/" + READ_OPERATION_1_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation2FileReader = new CsvFileReader(new File(parametersDir.getAbsolutePath() + "/" + READ_OPERATION_2_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation3FileReader = new CsvFileReader(new File(parametersDir.getAbsolutePath() + "/" + READ_OPERATION_3_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation4FileReader = new CsvFileReader(new File(parametersDir.getAbsolutePath() + "/" + READ_OPERATION_4_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation5FileReader = new CsvFileReader(new File(parametersDir.getAbsolutePath() + "/" + READ_OPERATION_5_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation6FileReader = new CsvFileReader(new File(parametersDir.getAbsolutePath() + "/" + READ_OPERATION_6_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation7FileReader = new CsvFileReader(new File(parametersDir.getAbsolutePath() + "/" + READ_OPERATION_7_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation8FileReader = new CsvFileReader(new File(parametersDir.getAbsolutePath() + "/" + READ_OPERATION_8_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation9FileReader = new CsvFileReader(new File(parametersDir.getAbsolutePath() + "/" + READ_OPERATION_9_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation10FileReader = new CsvFileReader(new File(parametersDir.getAbsolutePath() + "/" + READ_OPERATION_10_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation11FileReader = new CsvFileReader(new File(parametersDir.getAbsolutePath() + "/" + READ_OPERATION_11_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation12FileReader = new CsvFileReader(new File(parametersDir.getAbsolutePath() + "/" + READ_OPERATION_12_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation13FileReader = new CsvFileReader(new File(parametersDir.getAbsolutePath() + "/" + READ_OPERATION_13_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation14FileReader = new CsvFileReader(new File(parametersDir.getAbsolutePath() + "/" + READ_OPERATION_14_PARAMS_FILENAME), CSV_SEPARATOR);
        } catch (FileNotFoundException e) {
            throw new WorkloadException("Unable to load one of the read operation parameters files", e);
        }

        try {
            long interleaveDurationMs = Long.parseLong(properties.get(INTERLEAVE_DURATION_KEY));
            interleaveDuration = Duration.fromMilli(interleaveDurationMs);
        } catch (NumberFormatException e) {
            throw new WorkloadException(String.format("Unable to parse interleave duration: %s", properties.get(INTERLEAVE_DURATION_KEY)), e);
        }

        try {
            readRatio = Double.parseDouble(properties.get(READ_RATIO_KEY));
        } catch (NumberFormatException e) {
            throw new WorkloadException(String.format("Unable to parse read ratio: %s", properties.get(READ_RATIO_KEY)), e);
        }

        try {
            writeRatio = Double.parseDouble(properties.get(WRITE_RATIO_KEY));
        } catch (NumberFormatException e) {
            throw new WorkloadException(String.format("Unable to parse write ratio: %s", properties.get(WRITE_RATIO_KEY)), e);
        }

        readOperationRatios = new HashMap<>();
        for (String readOperationKey : READ_OPERATION_KEYS) {
            String operationRatioString = properties.get(readOperationKey);
            Double operationRatio = Double.parseDouble(operationRatioString);
            try {
                Class operationClass = ClassLoaderHelper.loadClass(readOperationKey);
                readOperationRatios.put(operationClass, operationRatio);
            } catch (ClassLoadingException e) {
                throw new WorkloadException(String.format("Unable to load operation class: %s", readOperationKey), e);
            }
        }

        writeOperationFilter = new HashSet<>();
        for (String writeOperationKey : WRITE_OPERATION_KEYS) {
            String writeOperationEnabledString = properties.get(writeOperationKey);
            Boolean writeOperationEnabled = Boolean.parseBoolean(writeOperationEnabledString);
            try {
                Class operationClass = ClassLoaderHelper.loadClass(writeOperationKey);
                if (writeOperationEnabled) writeOperationFilter.add(operationClass);
            } catch (ClassLoadingException e) {
                throw new WorkloadException(String.format("Unable to load operation class: %s", writeOperationKey), e);
            }
        }
    }

    private List<String> missingPropertiesParameters
            (Map<String, String> properties, List<String> compulsoryPropertyKeys) {
        List<String> missingPropertyKeys = new ArrayList<>();
        for (String compulsoryKey : compulsoryPropertyKeys) {
            if (null == properties.get(compulsoryKey)) missingPropertyKeys.add(compulsoryKey);
        }
        return missingPropertyKeys;
    }

    @Override
    protected void onCleanup() throws WorkloadException {
        readOperation1FileReader.closeReader();
        readOperation2FileReader.closeReader();
        readOperation3FileReader.closeReader();
        readOperation4FileReader.closeReader();
        readOperation5FileReader.closeReader();
        readOperation6FileReader.closeReader();
        readOperation7FileReader.closeReader();
        readOperation8FileReader.closeReader();
        readOperation9FileReader.closeReader();
        readOperation10FileReader.closeReader();
        readOperation11FileReader.closeReader();
        readOperation12FileReader.closeReader();
        readOperation13FileReader.closeReader();
        readOperation14FileReader.closeReader();
    }

    @Override
    protected Iterator<Operation<?>> createOperations(GeneratorFactory generators) throws WorkloadException {
        /*
         * Create mix of read operation generators
         */
        List<Tuple2<Double, Iterator<Operation<?>>>> readOperationsMix = new ArrayList<>();
        readOperationsMix.add(Tuple.<Double, Iterator<Operation<?>>>tuple2(
                readOperationRatios.get(LdbcQuery1.class),
                new Query1EventStreamReader(generators.repeating(readOperation1FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH)));
        readOperationsMix.add(Tuple.<Double, Iterator<Operation<?>>>tuple2(
                readOperationRatios.get(LdbcQuery2.class),
                new Query2EventStreamReader(generators.repeating(readOperation2FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH)));
        readOperationsMix.add(Tuple.<Double, Iterator<Operation<?>>>tuple2(
                readOperationRatios.get(LdbcQuery3.class),
                new Query3EventStreamReader(generators.repeating(readOperation3FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH)));
        readOperationsMix.add(Tuple.<Double, Iterator<Operation<?>>>tuple2(
                readOperationRatios.get(LdbcQuery4.class),
                new Query4EventStreamReader(generators.repeating(readOperation4FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH)));
        readOperationsMix.add(Tuple.<Double, Iterator<Operation<?>>>tuple2(
                readOperationRatios.get(LdbcQuery5.class),
                new Query5EventStreamReader(generators.repeating(readOperation5FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH)));
        readOperationsMix.add(Tuple.<Double, Iterator<Operation<?>>>tuple2(
                readOperationRatios.get(LdbcQuery6.class),
                new Query6EventStreamReader(generators.repeating(readOperation6FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH)));
        readOperationsMix.add(Tuple.<Double, Iterator<Operation<?>>>tuple2(
                readOperationRatios.get(LdbcQuery7.class),
                new Query7EventStreamReader(generators.repeating(readOperation7FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH)));
        readOperationsMix.add(Tuple.<Double, Iterator<Operation<?>>>tuple2(
                readOperationRatios.get(LdbcQuery8.class),
                new Query8EventStreamReader(generators.repeating(readOperation8FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH)));
        readOperationsMix.add(Tuple.<Double, Iterator<Operation<?>>>tuple2(
                readOperationRatios.get(LdbcQuery9.class),
                new Query9EventStreamReader(generators.repeating(readOperation9FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH)));
        readOperationsMix.add(Tuple.<Double, Iterator<Operation<?>>>tuple2(
                readOperationRatios.get(LdbcQuery10.class),
                new Query10EventStreamReader(generators.repeating(readOperation10FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH)));
        readOperationsMix.add(Tuple.<Double, Iterator<Operation<?>>>tuple2(
                readOperationRatios.get(LdbcQuery11.class),
                new Query11EventStreamReader(generators.repeating(readOperation11FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH)));
        readOperationsMix.add(Tuple.<Double, Iterator<Operation<?>>>tuple2(
                readOperationRatios.get(LdbcQuery12.class),
                new Query12EventStreamReader(generators.repeating(readOperation12FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH)));
        readOperationsMix.add(Tuple.<Double, Iterator<Operation<?>>>tuple2(
                readOperationRatios.get(LdbcQuery13.class),
                new Query13EventStreamReader(generators.repeating(readOperation13FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH)));
        readOperationsMix.add(Tuple.<Double, Iterator<Operation<?>>>tuple2(
                readOperationRatios.get(LdbcQuery14.class),
                new Query14EventStreamReader(generators.repeating(readOperation14FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH)));
        /*
         * Create discrete generator to choose operations from read mix
         */
        Iterator<Operation<?>> readOperations = generators.weightedDiscreteDereferencing(readOperationsMix);

        // TODO parameter for: first read operation start time, as duration from beginning of benchmark run
        // TODO parameter for: interleave duration per read operation
        /*
         * Add scheduled start time to read queries
         */
        Iterator<Time> startTimeGenerator = GeneratorUtils.constantIncrementStartTimeGenerator(generators, Time.now(), interleaveDuration);
        Iterator<Operation<?>> readOperationsWithTime = new StartTimeAssigningOperationGenerator(startTimeGenerator, readOperations);

        /*
         * Filter Write Operations
         */
        Predicate<Operation<?>> allowedWriteOperationsFilter = new Predicate<Operation<?>>() {
            @Override
            public boolean apply(Operation<?> operation) {
                return writeOperationFilter.contains(operation.getClass());
            }
        };

        Iterator<Operation<?>> filteredWriteOperations = Iterators.filter(writeOperationsReader, allowedWriteOperationsFilter);

        /*
         * Move scheduled start times of write operations to now
         */
        // TODO parameter for: first write operation start time, as duration from beginning of benchmark run
        // TODO parameter for: write operation compression/expansion, this then needs to be applied to DeltaTime too (at present this is not done)
        Iterator<Operation<?>> filteredWriteOperationsTimeShiftedToNow = generators.timeOffset(filteredWriteOperations, Time.now());

        // TODO this is not how operations will be mixed
//        /*
//         * Mix read and write operations
//         */
//        List<Tuple2<Double, Iterator<Operation<?>>>> readWriteOperationMix = Lists.newArrayList(
//                Tuple.tuple2(readRatio, readOperationsWithTime),
//                Tuple.tuple2(writeRatio, filteredWriteOperationsTimeShiftedToNow)
//        );
//        Iterator<Operation<?>> readAndWriteOperations = generators.weightedDiscreteDereferencing(readWriteOperationMix);
        // TODO temporary solution. better solution will look similar, but will merge more streams - one per read operation stream (14) + one per write stream stream (0..n)
        Iterator<Operation<?>> readAndWriteOperations = generators.mergeSortOperationsByScheduledStartTime(readOperationsWithTime, filteredWriteOperationsTimeShiftedToNow);

        return readAndWriteOperations;
    }
}
