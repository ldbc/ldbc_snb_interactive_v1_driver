package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationClassification;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.ClassLoadingException;
import com.ldbc.driver.util.CsvFileReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

import static com.ldbc.driver.OperationClassification.GctMode;
import static com.ldbc.driver.OperationClassification.SchedulingMode;
import static com.ldbc.driver.generator.CsvEventStreamReader.EventReturnPolicy;

public class LdbcInteractiveWorkload extends Workload {
    // TODO add?
//    public final static String MIN_WRITE_EVENT_START_TIME = "min_write_event_start_time";
//    public final static String MAX_WRITE_EVENT_START_TIME = "max_write_event_start_time";

    public final static String DATA_DIRECTORY = "data_dir";
    public final static String PARAMETERS_DIRECTORY = "parameters_dir";
    private final static String LDBC_INTERACTIVE_PACKAGE_PREFIX = removeSuffix(LdbcQuery1.class.getName(), LdbcQuery1.class.getSimpleName());

    /*
     * Operation Interleave
     */
    private final static String INTERLEAVE_SUFFIX = "_interleave";
    public final static String READ_OPERATION_1_INTERLEAVE_KEY = LdbcQuery1.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_2_INTERLEAVE_KEY = LdbcQuery2.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_3_INTERLEAVE_KEY = LdbcQuery3.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_4_INTERLEAVE_KEY = LdbcQuery4.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_5_INTERLEAVE_KEY = LdbcQuery5.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_6_INTERLEAVE_KEY = LdbcQuery6.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_7_INTERLEAVE_KEY = LdbcQuery7.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_8_INTERLEAVE_KEY = LdbcQuery8.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_9_INTERLEAVE_KEY = LdbcQuery9.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_10_INTERLEAVE_KEY = LdbcQuery10.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_11_INTERLEAVE_KEY = LdbcQuery11.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_12_INTERLEAVE_KEY = LdbcQuery12.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_13_INTERLEAVE_KEY = LdbcQuery13.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_14_INTERLEAVE_KEY = LdbcQuery14.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static List<String> READ_OPERATION_INTERLEAVE_KEYS = Lists.newArrayList(
            READ_OPERATION_1_INTERLEAVE_KEY,
            READ_OPERATION_2_INTERLEAVE_KEY,
            READ_OPERATION_3_INTERLEAVE_KEY,
            READ_OPERATION_4_INTERLEAVE_KEY,
            READ_OPERATION_5_INTERLEAVE_KEY,
            READ_OPERATION_6_INTERLEAVE_KEY,
            READ_OPERATION_7_INTERLEAVE_KEY,
            READ_OPERATION_8_INTERLEAVE_KEY,
            READ_OPERATION_9_INTERLEAVE_KEY,
            READ_OPERATION_10_INTERLEAVE_KEY,
            READ_OPERATION_11_INTERLEAVE_KEY,
            READ_OPERATION_12_INTERLEAVE_KEY,
            READ_OPERATION_13_INTERLEAVE_KEY,
            READ_OPERATION_14_INTERLEAVE_KEY);

    /*
     * Operation Enable
     */
    private final static String ENABLE_SUFFIX = "_enable";
    public final static String READ_OPERATION_1_ENABLE_KEY = LdbcQuery1.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_2_ENABLE_KEY = LdbcQuery2.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_3_ENABLE_KEY = LdbcQuery3.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_4_ENABLE_KEY = LdbcQuery4.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_5_ENABLE_KEY = LdbcQuery5.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_6_ENABLE_KEY = LdbcQuery6.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_7_ENABLE_KEY = LdbcQuery7.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_8_ENABLE_KEY = LdbcQuery8.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_9_ENABLE_KEY = LdbcQuery9.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_10_ENABLE_KEY = LdbcQuery10.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_11_ENABLE_KEY = LdbcQuery11.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_12_ENABLE_KEY = LdbcQuery12.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_13_ENABLE_KEY = LdbcQuery13.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_14_ENABLE_KEY = LdbcQuery14.class.getSimpleName() + ENABLE_SUFFIX;
    public final static List<String> READ_OPERATION_ENABLE_KEYS = Lists.newArrayList(
            READ_OPERATION_1_ENABLE_KEY,
            READ_OPERATION_2_ENABLE_KEY,
            READ_OPERATION_3_ENABLE_KEY,
            READ_OPERATION_4_ENABLE_KEY,
            READ_OPERATION_5_ENABLE_KEY,
            READ_OPERATION_6_ENABLE_KEY,
            READ_OPERATION_7_ENABLE_KEY,
            READ_OPERATION_8_ENABLE_KEY,
            READ_OPERATION_9_ENABLE_KEY,
            READ_OPERATION_10_ENABLE_KEY,
            READ_OPERATION_11_ENABLE_KEY,
            READ_OPERATION_12_ENABLE_KEY,
            READ_OPERATION_13_ENABLE_KEY,
            READ_OPERATION_14_ENABLE_KEY);
    public final static String WRITE_OPERATION_1_ENABLE_KEY = LdbcUpdate1AddPerson.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String WRITE_OPERATION_2_ENABLE_KEY = LdbcUpdate2AddPostLike.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String WRITE_OPERATION_3_ENABLE_KEY = LdbcUpdate3AddCommentLike.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String WRITE_OPERATION_4_ENABLE_KEY = LdbcUpdate4AddForum.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String WRITE_OPERATION_5_ENABLE_KEY = LdbcUpdate5AddForumMembership.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String WRITE_OPERATION_6_ENABLE_KEY = LdbcUpdate6AddPost.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String WRITE_OPERATION_7_ENABLE_KEY = LdbcUpdate7AddComment.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String WRITE_OPERATION_8_ENABLE_KEY = LdbcUpdate8AddFriendship.class.getSimpleName() + ENABLE_SUFFIX;
    public final static List<String> WRITE_OPERATION_ENABLE_KEYS = Lists.newArrayList(
            WRITE_OPERATION_1_ENABLE_KEY,
            WRITE_OPERATION_2_ENABLE_KEY,
            WRITE_OPERATION_3_ENABLE_KEY,
            WRITE_OPERATION_4_ENABLE_KEY,
            WRITE_OPERATION_5_ENABLE_KEY,
            WRITE_OPERATION_6_ENABLE_KEY,
            WRITE_OPERATION_7_ENABLE_KEY,
            WRITE_OPERATION_8_ENABLE_KEY
    );

    /*
     * Read Operation Parameters
     */
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

    /*
     * Write Operation Parameters
     */
    public final static String WRITE_OPERATIONS_FILENAME = "updateStream_0.csv";

    private final static String CSV_SEPARATOR = "\\|";

    private CsvFileReader writeOperationsFileReader;

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

    private Duration readOperation1Interleave;
    private Duration readOperation2Interleave;
    private Duration readOperation3Interleave;
    private Duration readOperation4Interleave;
    private Duration readOperation5Interleave;
    private Duration readOperation6Interleave;
    private Duration readOperation7Interleave;
    private Duration readOperation8Interleave;
    private Duration readOperation9Interleave;
    private Duration readOperation10Interleave;
    private Duration readOperation11Interleave;
    private Duration readOperation12Interleave;
    private Duration readOperation13Interleave;
    private Duration readOperation14Interleave;

    private Map<Class, Duration> readOperationInterleaves;
    private Set<Class> readOperationFilter;
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
    public void onInit(Map<String, String> params) throws WorkloadException {
        List<String> compulsoryKeys = Lists.newArrayList(
                DATA_DIRECTORY,
                PARAMETERS_DIRECTORY);
        compulsoryKeys.addAll(READ_OPERATION_INTERLEAVE_KEYS);
        compulsoryKeys.addAll(READ_OPERATION_ENABLE_KEYS);
        compulsoryKeys.addAll(WRITE_OPERATION_ENABLE_KEYS);

        List<String> missingPropertyParameters = missingPropertiesParameters(params, compulsoryKeys);
        if (false == missingPropertyParameters.isEmpty())
            throw new WorkloadException(String.format("Workload could not initialize due to missing parameters: %s", missingPropertyParameters.toString()));

        File dataDir = new File(params.get(DATA_DIRECTORY));
        if (false == dataDir.exists()) {
            throw new WorkloadException(String.format("Data directory does not exist: %s", dataDir.getAbsolutePath()));
        }
        File writeOperationsFile = new File(dataDir, WRITE_OPERATIONS_FILENAME);
        if (false == writeOperationsFile.exists()) {
            throw new WorkloadException(String.format("Write events file does not exist: %s", writeOperationsFile.getAbsolutePath()));
        }
        try {
            writeOperationsFileReader = new CsvFileReader(writeOperationsFile, CSV_SEPARATOR);
        } catch (FileNotFoundException e) {
            throw new WorkloadException("Unable to load write operation parameters file", e);
        }

        File parametersDir = new File(params.get(PARAMETERS_DIRECTORY));
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
            readOperation1FileReader = new CsvFileReader(new File(parametersDir, READ_OPERATION_1_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation2FileReader = new CsvFileReader(new File(parametersDir, READ_OPERATION_2_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation3FileReader = new CsvFileReader(new File(parametersDir, READ_OPERATION_3_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation4FileReader = new CsvFileReader(new File(parametersDir, READ_OPERATION_4_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation5FileReader = new CsvFileReader(new File(parametersDir, READ_OPERATION_5_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation6FileReader = new CsvFileReader(new File(parametersDir, READ_OPERATION_6_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation7FileReader = new CsvFileReader(new File(parametersDir, READ_OPERATION_7_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation8FileReader = new CsvFileReader(new File(parametersDir, READ_OPERATION_8_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation9FileReader = new CsvFileReader(new File(parametersDir, READ_OPERATION_9_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation10FileReader = new CsvFileReader(new File(parametersDir, READ_OPERATION_10_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation11FileReader = new CsvFileReader(new File(parametersDir, READ_OPERATION_11_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation12FileReader = new CsvFileReader(new File(parametersDir, READ_OPERATION_12_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation13FileReader = new CsvFileReader(new File(parametersDir, READ_OPERATION_13_PARAMS_FILENAME), CSV_SEPARATOR);
            readOperation14FileReader = new CsvFileReader(new File(parametersDir, READ_OPERATION_14_PARAMS_FILENAME), CSV_SEPARATOR);
        } catch (FileNotFoundException e) {
            throw new WorkloadException("Unable to load one of the read operation parameters files", e);
        }

        try {
            readOperation1Interleave = Duration.fromMilli(Long.parseLong(params.get(READ_OPERATION_1_INTERLEAVE_KEY)));
            readOperation2Interleave = Duration.fromMilli(Long.parseLong(params.get(READ_OPERATION_2_INTERLEAVE_KEY)));
            readOperation3Interleave = Duration.fromMilli(Long.parseLong(params.get(READ_OPERATION_3_INTERLEAVE_KEY)));
            readOperation4Interleave = Duration.fromMilli(Long.parseLong(params.get(READ_OPERATION_4_INTERLEAVE_KEY)));
            readOperation5Interleave = Duration.fromMilli(Long.parseLong(params.get(READ_OPERATION_5_INTERLEAVE_KEY)));
            readOperation6Interleave = Duration.fromMilli(Long.parseLong(params.get(READ_OPERATION_6_INTERLEAVE_KEY)));
            readOperation7Interleave = Duration.fromMilli(Long.parseLong(params.get(READ_OPERATION_7_INTERLEAVE_KEY)));
            readOperation8Interleave = Duration.fromMilli(Long.parseLong(params.get(READ_OPERATION_8_INTERLEAVE_KEY)));
            readOperation9Interleave = Duration.fromMilli(Long.parseLong(params.get(READ_OPERATION_9_INTERLEAVE_KEY)));
            readOperation10Interleave = Duration.fromMilli(Long.parseLong(params.get(READ_OPERATION_10_INTERLEAVE_KEY)));
            readOperation11Interleave = Duration.fromMilli(Long.parseLong(params.get(READ_OPERATION_11_INTERLEAVE_KEY)));
            readOperation12Interleave = Duration.fromMilli(Long.parseLong(params.get(READ_OPERATION_12_INTERLEAVE_KEY)));
            readOperation13Interleave = Duration.fromMilli(Long.parseLong(params.get(READ_OPERATION_13_INTERLEAVE_KEY)));
            readOperation14Interleave = Duration.fromMilli(Long.parseLong(params.get(READ_OPERATION_14_INTERLEAVE_KEY)));
        } catch (NumberFormatException e) {
            throw new WorkloadException("Unable to parse one of the read operation interleave values", e);
        }

        readOperationInterleaves = new HashMap<>();
        for (String readOperationInterleaveKey : READ_OPERATION_INTERLEAVE_KEYS) {
            String readOperationInterleaveString = params.get(readOperationInterleaveKey);
            Duration readOperationInterleaveDuration = Duration.fromMilli(Long.parseLong(readOperationInterleaveString));
            try {
                String readOperationClassName = LDBC_INTERACTIVE_PACKAGE_PREFIX + removeSuffix(readOperationInterleaveKey, INTERLEAVE_SUFFIX);
                Class readOperationClass = ClassLoaderHelper.loadClass(readOperationClassName);
                readOperationInterleaves.put(readOperationClass, readOperationInterleaveDuration);
            } catch (ClassLoadingException e) {
                throw new WorkloadException(String.format("Unable to load operation class: %s", readOperationInterleaveKey), e);
            }
        }

        readOperationFilter = new HashSet<>();
        for (String readOperationEnableKey : READ_OPERATION_ENABLE_KEYS) {
            String readOperationEnabledString = params.get(readOperationEnableKey);
            Boolean readOperationEnabled = Boolean.parseBoolean(readOperationEnabledString);
            try {
                String readOperationClassName = LDBC_INTERACTIVE_PACKAGE_PREFIX + removeSuffix(readOperationEnableKey, ENABLE_SUFFIX);
                Class readOperationClass = ClassLoaderHelper.loadClass(readOperationClassName);
                if (readOperationEnabled) readOperationFilter.add(readOperationClass);
            } catch (ClassLoadingException e) {
                throw new WorkloadException(String.format("Unable to load operation class: %s", readOperationEnableKey), e);
            }
        }

        writeOperationFilter = new HashSet<>();
        for (String writeOperationEnableKey : WRITE_OPERATION_ENABLE_KEYS) {
            String writeOperationEnabledString = params.get(writeOperationEnableKey);
            Boolean writeOperationEnabled = Boolean.parseBoolean(writeOperationEnabledString);
            try {
                String writeOperationClassName = LDBC_INTERACTIVE_PACKAGE_PREFIX + removeSuffix(writeOperationEnableKey, ENABLE_SUFFIX);
                Class writeOperationClass = ClassLoaderHelper.loadClass(writeOperationClassName);
                if (writeOperationEnabled) writeOperationFilter.add(writeOperationClass);
            } catch (ClassLoadingException e) {
                throw new WorkloadException(String.format("Unable to load operation class: %s", writeOperationEnableKey), e);
            }
        }
    }

    private static String removeSuffix(String original, String suffix) {
        return (original.indexOf(suffix) == -1) ? original : original.substring(0, original.lastIndexOf(suffix));
    }

    private static List<String> missingPropertiesParameters
            (Map<String, String> properties, List<String> compulsoryPropertyKeys) {
        List<String> missingPropertyKeys = new ArrayList<>();
        for (String compulsoryKey : compulsoryPropertyKeys) {
            if (null == properties.get(compulsoryKey)) missingPropertyKeys.add(compulsoryKey);
        }
        return missingPropertyKeys;
    }

    @Override
    protected void onCleanup() throws WorkloadException {
        writeOperationsFileReader.closeReader();
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
        // TODO assign this in a better way
        Time workloadStartTime = Time.now();

        /*
         * Create read operation streams, with specified interleaves
         */
        Iterator<Operation<?>> operation1StreamWithoutTimes = new Query1EventStreamReader(generators.repeating(readOperation1FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation1FromWorkloadStart = Duration.fromSeconds(1);
        Iterator<Time> operation1StartTimes = generators.constantIncrementTime(workloadStartTime.plus(firstOperation1FromWorkloadStart), readOperation1Interleave);
        Iterator<Operation<?>> readOperation1Stream = generators.startTimeAssigning(operation1StartTimes, operation1StreamWithoutTimes);

        Iterator<Operation<?>> operation2StreamWithoutTimes = new Query2EventStreamReader(generators.repeating(readOperation2FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation2FromWorkloadStart = Duration.fromSeconds(1);
        Iterator<Time> operation2StartTimes = generators.constantIncrementTime(workloadStartTime.plus(firstOperation2FromWorkloadStart), readOperation2Interleave);
        Iterator<Operation<?>> readOperation2Stream = generators.startTimeAssigning(operation2StartTimes, operation2StreamWithoutTimes);

        Iterator<Operation<?>> operation3StreamWithoutTimes = new Query3EventStreamReader(generators.repeating(readOperation3FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation3FromWorkloadStart = Duration.fromSeconds(1);
        Iterator<Time> operation3StartTimes = generators.constantIncrementTime(workloadStartTime.plus(firstOperation3FromWorkloadStart), readOperation3Interleave);
        Iterator<Operation<?>> readOperation3Stream = generators.startTimeAssigning(operation3StartTimes, operation3StreamWithoutTimes);

        Iterator<Operation<?>> operation4StreamWithoutTimes = new Query4EventStreamReader(generators.repeating(readOperation4FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation4FromWorkloadStart = Duration.fromSeconds(1);
        Iterator<Time> operation4StartTimes = generators.constantIncrementTime(workloadStartTime.plus(firstOperation4FromWorkloadStart), readOperation4Interleave);
        Iterator<Operation<?>> readOperation4Stream = generators.startTimeAssigning(operation4StartTimes, operation4StreamWithoutTimes);

        Iterator<Operation<?>> operation5StreamWithoutTimes = new Query5EventStreamReader(generators.repeating(readOperation5FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation5FromWorkloadStart = Duration.fromSeconds(1);
        Iterator<Time> operation5StartTimes = generators.constantIncrementTime(workloadStartTime.plus(firstOperation5FromWorkloadStart), readOperation5Interleave);
        Iterator<Operation<?>> readOperation5Stream = generators.startTimeAssigning(operation5StartTimes, operation5StreamWithoutTimes);

        Iterator<Operation<?>> operation6StreamWithoutTimes = new Query6EventStreamReader(generators.repeating(readOperation6FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation6FromWorkloadStart = Duration.fromSeconds(1);
        Iterator<Time> operation6StartTimes = generators.constantIncrementTime(workloadStartTime.plus(firstOperation6FromWorkloadStart), readOperation6Interleave);
        Iterator<Operation<?>> readOperation6Stream = generators.startTimeAssigning(operation6StartTimes, operation6StreamWithoutTimes);

        Iterator<Operation<?>> operation7StreamWithoutTimes = new Query7EventStreamReader(generators.repeating(readOperation7FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation7FromWorkloadStart = Duration.fromSeconds(1);
        Iterator<Time> operation7StartTimes = generators.constantIncrementTime(workloadStartTime.plus(firstOperation7FromWorkloadStart), readOperation7Interleave);
        Iterator<Operation<?>> readOperation7Stream = generators.startTimeAssigning(operation7StartTimes, operation7StreamWithoutTimes);

        Iterator<Operation<?>> operation8StreamWithoutTimes = new Query8EventStreamReader(generators.repeating(readOperation8FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation8FromWorkloadStart = Duration.fromSeconds(1);
        Iterator<Time> operation8StartTimes = generators.constantIncrementTime(workloadStartTime.plus(firstOperation8FromWorkloadStart), readOperation8Interleave);
        Iterator<Operation<?>> readOperation8Stream = generators.startTimeAssigning(operation8StartTimes, operation8StreamWithoutTimes);

        Iterator<Operation<?>> operation9StreamWithoutTimes = new Query9EventStreamReader(generators.repeating(readOperation9FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation9FromWorkloadStart = Duration.fromSeconds(1);
        Iterator<Time> operation9StartTimes = generators.constantIncrementTime(workloadStartTime.plus(firstOperation9FromWorkloadStart), readOperation9Interleave);
        Iterator<Operation<?>> readOperation9Stream = generators.startTimeAssigning(operation9StartTimes, operation9StreamWithoutTimes);

        Iterator<Operation<?>> operation10StreamWithoutTimes = new Query10EventStreamReader(generators.repeating(readOperation10FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation10FromWorkloadStart = Duration.fromSeconds(1);
        Iterator<Time> operation10StartTimes = generators.constantIncrementTime(workloadStartTime.plus(firstOperation10FromWorkloadStart), readOperation10Interleave);
        Iterator<Operation<?>> readOperation10Stream = generators.startTimeAssigning(operation10StartTimes, operation10StreamWithoutTimes);

        Iterator<Operation<?>> operation11StreamWithoutTimes = new Query11EventStreamReader(generators.repeating(readOperation11FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation11FromWorkloadStart = Duration.fromSeconds(1);
        Iterator<Time> operation11StartTimes = generators.constantIncrementTime(workloadStartTime.plus(firstOperation11FromWorkloadStart), readOperation11Interleave);
        Iterator<Operation<?>> readOperation11Stream = generators.startTimeAssigning(operation11StartTimes, operation11StreamWithoutTimes);

        Iterator<Operation<?>> operation12StreamWithoutTimes = new Query12EventStreamReader(generators.repeating(readOperation12FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation12FromWorkloadStart = Duration.fromSeconds(1);
        Iterator<Time> operation12StartTimes = generators.constantIncrementTime(workloadStartTime.plus(firstOperation12FromWorkloadStart), readOperation12Interleave);
        Iterator<Operation<?>> readOperation12Stream = generators.startTimeAssigning(operation12StartTimes, operation12StreamWithoutTimes);

        Iterator<Operation<?>> operation13StreamWithoutTimes = new Query13EventStreamReader(generators.repeating(readOperation13FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation13FromWorkloadStart = Duration.fromSeconds(1);
        Iterator<Time> operation13StartTimes = generators.constantIncrementTime(workloadStartTime.plus(firstOperation13FromWorkloadStart), readOperation13Interleave);
        Iterator<Operation<?>> readOperation13Stream = generators.startTimeAssigning(operation13StartTimes, operation13StreamWithoutTimes);

        Iterator<Operation<?>> operation14StreamWithoutTimes = new Query14EventStreamReader(generators.repeating(readOperation14FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation14FromWorkloadStart = Duration.fromSeconds(1);
        Iterator<Time> operation14StartTimes = generators.constantIncrementTime(workloadStartTime.plus(firstOperation14FromWorkloadStart), readOperation14Interleave);
        Iterator<Operation<?>> readOperation14Stream = generators.startTimeAssigning(operation14StartTimes, operation14StreamWithoutTimes);

        /*
         * Create write operations stream
         */
        Iterator<Operation<?>> writeOperationsStream = new WriteEventStreamReader(writeOperationsFileReader, EventReturnPolicy.AT_LEAST_ONE_MATCH);

        // TODO parameter for: write operation compression/expansion, this then needs to be applied to DeltaTime too (at present this is not done)

        /*
         * Move scheduled start times of write operations to workload start time
         */
        // TODO add parameter, or do in more intelligent way
        Duration firstWriteOperationFromWorkloadStart = Duration.fromSeconds(1);
        Iterator<Operation<?>> writeOperationStream = generators.timeOffset(writeOperationsStream, workloadStartTime.plus(firstWriteOperationFromWorkloadStart));

        /*
         * Merge all operation streams, ordered by operation start times
         */
        Iterator<Operation<?>> readAndWriteOperations = generators.mergeSortOperationsByStartTime(
                writeOperationStream,
                readOperation1Stream,
                readOperation2Stream,
                readOperation3Stream,
                readOperation4Stream,
                readOperation5Stream,
                readOperation6Stream,
                readOperation7Stream,
                readOperation8Stream,
                readOperation9Stream,
                readOperation10Stream,
                readOperation11Stream,
                readOperation12Stream,
                readOperation13Stream,
                readOperation14Stream);

        /*
         * Filter Operations
         */
        Predicate<Operation<?>> allowedOperationsFilter = new Predicate<Operation<?>>() {
            @Override
            public boolean apply(Operation<?> operation) {
                return writeOperationFilter.contains(operation.getClass()) || readOperationFilter.contains(operation.getClass());
            }
        };
        Iterator<Operation<?>> filteredOperations = Iterators.filter(readAndWriteOperations, allowedOperationsFilter);

        return filteredOperations;
    }
}
