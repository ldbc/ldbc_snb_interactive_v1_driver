package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.ClassLoadingException;
import com.ldbc.driver.util.CsvFileReader;
import com.ldbc.driver.util.Function1;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import static com.ldbc.driver.OperationClassification.SchedulingMode;
import static com.ldbc.driver.generator.CsvEventStreamReader.EventReturnPolicy;

public class LdbcSnbInteractiveWorkload extends Workload {
    public static Map<String, String> defaultConfig() {
        Map<String, String> params = new HashMap<>();
        // General Driver parameters
        params.put(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "1000");
        params.put(ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, LdbcSnbInteractiveWorkload.class.getName());
        params.put(ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_ARG, "ldbc_socnet_interactive_results.json");
        // LDBC Interactive Workload-specific parameters
        // reads
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_1_INTERLEAVE_KEY, "30");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_2_INTERLEAVE_KEY, "12");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_3_INTERLEAVE_KEY, "72");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_4_INTERLEAVE_KEY, "27");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_5_INTERLEAVE_KEY, "42");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_6_INTERLEAVE_KEY, "18");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_7_INTERLEAVE_KEY, "13");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_8_INTERLEAVE_KEY, "1");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_9_INTERLEAVE_KEY, "40");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_10_INTERLEAVE_KEY, "27");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_11_INTERLEAVE_KEY, "18");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_12_INTERLEAVE_KEY, "34");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_13_INTERLEAVE_KEY, "1");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_14_INTERLEAVE_KEY, "66");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_1_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_2_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_3_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_4_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_5_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_6_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_7_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_8_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_9_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_10_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_11_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_12_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_13_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_14_ENABLE_KEY, "true");
        // writes
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_1_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_2_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_3_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_4_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_5_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_6_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_7_ENABLE_KEY, "true");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_8_ENABLE_KEY, "true");
        return ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys(params);
    }

    public static Map<String, String> defaultReadOnlyConfig() {
        Map<String, String> params = defaultConfig();
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_1_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_2_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_3_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_4_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_5_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_6_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_7_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.WRITE_OPERATION_8_ENABLE_KEY, "false");
        return ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys(params);
    }

    public static Map<String, String> defaultWriteOnlyConfig() {
        Map<String, String> params = defaultConfig();
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_1_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_2_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_3_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_4_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_5_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_6_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_7_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_8_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_9_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_10_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_11_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_12_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_13_ENABLE_KEY, "false");
        params.put(LdbcSnbInteractiveWorkload.READ_OPERATION_14_ENABLE_KEY, "false");
        return ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys(params);
    }

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

    private CsvFileReader writeOperationsFileReader = null;

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

    private Map<Class<? extends Operation>, OperationClassification> operationClassifications;

    @Override
    public Map<Class<? extends Operation>, OperationClassification> getOperationClassifications() {
        return operationClassifications;
    }

    @Override
    public void onInit(Map<String, String> params) throws WorkloadException {
        List<String> compulsoryKeys = Lists.newArrayList(
                PARAMETERS_DIRECTORY);
        compulsoryKeys.addAll(READ_OPERATION_INTERLEAVE_KEYS);
        compulsoryKeys.addAll(READ_OPERATION_ENABLE_KEYS);
        compulsoryKeys.addAll(WRITE_OPERATION_ENABLE_KEYS);

        List<String> missingPropertyParameters = missingPropertiesParameters(params, compulsoryKeys);
        if (false == missingPropertyParameters.isEmpty())
            throw new WorkloadException(String.format("Workload could not initialize due to missing parameters: %s", missingPropertyParameters.toString()));

        String dataDirPath = params.get(DATA_DIRECTORY);
        if (null != dataDirPath) {
            File dataDir = new File(dataDirPath);
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

        // Create classifications for all operations
        operationClassifications = new HashMap<>();
        /*
         * Modes (with examples from LDBC Interactive SNB Workload):
         * - WINDOWED               & NONE -------------------> n/a
         * - WINDOWED               & READ -------------------> Add Friendship
         * - WINDOWED               & READ WRITE -------------> Add Person
         * - INDIVIDUAL_BLOCKING    & NONE -------------------> n/a
         * - INDIVIDUAL_BLOCKING    & READ -------------------> Add Post
         *                                                      Add Comment
         *                                                      Add Post Like
         *                                                      Add Comment Like
         *                                                      Add Forum
         *                                                      Add Forum Membership
         * - INDIVIDUAL_BLOCKING    & READ WRITE -------------> n/a
         * - INDIVIDUAL_ASYNC       & NONE -------------------> Reads 1-14
         * - INDIVIDUAL_ASYNC       & READ -------------------> n/a
         * - INDIVIDUAL_ASYNC       & READ WRITE -------------> n/a
        */
        operationClassifications.put(LdbcQuery1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
        operationClassifications.put(LdbcQuery2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
        operationClassifications.put(LdbcQuery3.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
        operationClassifications.put(LdbcQuery4.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
        operationClassifications.put(LdbcQuery5.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
        operationClassifications.put(LdbcQuery6.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
        operationClassifications.put(LdbcQuery7.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
        operationClassifications.put(LdbcQuery8.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
        operationClassifications.put(LdbcQuery9.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
        operationClassifications.put(LdbcQuery10.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
        operationClassifications.put(LdbcQuery11.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
        operationClassifications.put(LdbcQuery12.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
        operationClassifications.put(LdbcQuery13.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
        operationClassifications.put(LdbcQuery14.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
        // TODO should this be ASYNC or WINDOWED?
        operationClassifications.put(LdbcUpdate1AddPerson.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.READ_WRITE));
        operationClassifications.put(LdbcUpdate2AddPostLike.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, OperationClassification.DependencyMode.READ));
        operationClassifications.put(LdbcUpdate3AddCommentLike.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, OperationClassification.DependencyMode.READ));
        operationClassifications.put(LdbcUpdate4AddForum.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, OperationClassification.DependencyMode.READ));
        operationClassifications.put(LdbcUpdate5AddForumMembership.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, OperationClassification.DependencyMode.READ));
        operationClassifications.put(LdbcUpdate6AddPost.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, OperationClassification.DependencyMode.READ));
        operationClassifications.put(LdbcUpdate7AddComment.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, OperationClassification.DependencyMode.READ));
        // TODO the planned scheduling mode for this is WINDOWED but ASYNC is simpler at this stage, and still correct - WINDOWED is an performance optimization only
//        operationClassifications.put(LdbcUpdate8AddFriendship.class, new OperationClassification(SchedulingMode.WINDOWED, GctMode.READ));
        operationClassifications.put(LdbcUpdate8AddFriendship.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.READ));

        // Filter out classifications for operations that are not enabled
        List<Class> operationTypes = Lists.<Class>newArrayList(operationClassifications.keySet());
        for (Class operationType : operationTypes) {
            if (false == readOperationFilter.contains(operationType) && false == writeOperationFilter.contains(operationType))
                operationClassifications.remove(operationType);
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
        if (null != writeOperationsFileReader) writeOperationsFileReader.closeReader();
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
    protected Iterator<Operation<?>> getOperations(GeneratorFactory gf) throws WorkloadException {
        // this is an arbitrary point in time that is simply used as reference point, Client will move times to the present anyway
        Time workloadStartTime = Time.fromMilli(0);

        /*
         * Create read operation streams, with specified interleaves
         */
        Iterator<Operation<?>> operation1StreamWithoutTimes = new Query1EventStreamReader(gf.repeating(readOperation1FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation1FromWorkloadStart = readOperation1Interleave;
        Iterator<Time> operation1StartTimes = gf.constantIncrementTime(workloadStartTime.plus(firstOperation1FromWorkloadStart), readOperation1Interleave);
        Iterator<Operation<?>> readOperation1Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation1StartTimes, operation1StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation2StreamWithoutTimes = new Query2EventStreamReader(gf.repeating(readOperation2FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation2FromWorkloadStart = readOperation2Interleave;
        Iterator<Time> operation2StartTimes = gf.constantIncrementTime(workloadStartTime.plus(firstOperation2FromWorkloadStart), readOperation2Interleave);
        Iterator<Operation<?>> readOperation2Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation2StartTimes, operation2StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation3StreamWithoutTimes = new Query3EventStreamReader(gf.repeating(readOperation3FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation3FromWorkloadStart = readOperation3Interleave;
        Iterator<Time> operation3StartTimes = gf.constantIncrementTime(workloadStartTime.plus(firstOperation3FromWorkloadStart), readOperation3Interleave);
        Iterator<Operation<?>> readOperation3Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation3StartTimes, operation3StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation4StreamWithoutTimes = new Query4EventStreamReader(gf.repeating(readOperation4FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation4FromWorkloadStart = readOperation4Interleave;
        Iterator<Time> operation4StartTimes = gf.constantIncrementTime(workloadStartTime.plus(firstOperation4FromWorkloadStart), readOperation4Interleave);
        Iterator<Operation<?>> readOperation4Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation4StartTimes, operation4StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation5StreamWithoutTimes = new Query5EventStreamReader(gf.repeating(readOperation5FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation5FromWorkloadStart = readOperation5Interleave;
        Iterator<Time> operation5StartTimes = gf.constantIncrementTime(workloadStartTime.plus(firstOperation5FromWorkloadStart), readOperation5Interleave);
        Iterator<Operation<?>> readOperation5Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation5StartTimes, operation5StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation6StreamWithoutTimes = new Query6EventStreamReader(gf.repeating(readOperation6FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation6FromWorkloadStart = readOperation6Interleave;
        Iterator<Time> operation6StartTimes = gf.constantIncrementTime(workloadStartTime.plus(firstOperation6FromWorkloadStart), readOperation6Interleave);
        Iterator<Operation<?>> readOperation6Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation6StartTimes, operation6StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation7StreamWithoutTimes = new Query7EventStreamReader(gf.repeating(readOperation7FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation7FromWorkloadStart = readOperation7Interleave;
        Iterator<Time> operation7StartTimes = gf.constantIncrementTime(workloadStartTime.plus(firstOperation7FromWorkloadStart), readOperation7Interleave);
        Iterator<Operation<?>> readOperation7Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation7StartTimes, operation7StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation8StreamWithoutTimes = new Query8EventStreamReader(gf.repeating(readOperation8FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation8FromWorkloadStart = readOperation8Interleave;
        Iterator<Time> operation8StartTimes = gf.constantIncrementTime(workloadStartTime.plus(firstOperation8FromWorkloadStart), readOperation8Interleave);
        Iterator<Operation<?>> readOperation8Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation8StartTimes, operation8StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation9StreamWithoutTimes = new Query9EventStreamReader(gf.repeating(readOperation9FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation9FromWorkloadStart = readOperation9Interleave;
        Iterator<Time> operation9StartTimes = gf.constantIncrementTime(workloadStartTime.plus(firstOperation9FromWorkloadStart), readOperation9Interleave);
        Iterator<Operation<?>> readOperation9Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation9StartTimes, operation9StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation10StreamWithoutTimes = new Query10EventStreamReader(gf.repeating(readOperation10FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation10FromWorkloadStart = readOperation10Interleave;
        Iterator<Time> operation10StartTimes = gf.constantIncrementTime(workloadStartTime.plus(firstOperation10FromWorkloadStart), readOperation10Interleave);
        Iterator<Operation<?>> readOperation10Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation10StartTimes, operation10StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation11StreamWithoutTimes = new Query11EventStreamReader(gf.repeating(readOperation11FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation11FromWorkloadStart = readOperation11Interleave;
        Iterator<Time> operation11StartTimes = gf.constantIncrementTime(workloadStartTime.plus(firstOperation11FromWorkloadStart), readOperation11Interleave);
        Iterator<Operation<?>> readOperation11Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation11StartTimes, operation11StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation12StreamWithoutTimes = new Query12EventStreamReader(gf.repeating(readOperation12FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation12FromWorkloadStart = readOperation12Interleave;
        Iterator<Time> operation12StartTimes = gf.constantIncrementTime(workloadStartTime.plus(firstOperation12FromWorkloadStart), readOperation12Interleave);
        Iterator<Operation<?>> readOperation12Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation12StartTimes, operation12StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation13StreamWithoutTimes = new Query13EventStreamReader(gf.repeating(readOperation13FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation13FromWorkloadStart = readOperation13Interleave;
        Iterator<Time> operation13StartTimes = gf.constantIncrementTime(workloadStartTime.plus(firstOperation13FromWorkloadStart), readOperation13Interleave);
        Iterator<Operation<?>> readOperation13Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation13StartTimes, operation13StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation14StreamWithoutTimes = new Query14EventStreamReader(gf.repeating(readOperation14FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        // TODO add parameter, or do in more intelligent way
        Duration firstOperation14FromWorkloadStart = readOperation14Interleave;
        Iterator<Time> operation14StartTimes = gf.constantIncrementTime(workloadStartTime.plus(firstOperation14FromWorkloadStart), readOperation14Interleave);
        Iterator<Operation<?>> readOperation14Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation14StartTimes, operation14StreamWithoutTimes)
        );

        /*
         * Create write operations stream
         */
        Iterator<Operation<?>> unfilteredWriteOperationStream;
        if (null == writeOperationsFileReader)
            unfilteredWriteOperationStream = Lists.<Operation<?>>newArrayList().iterator();
        else
            unfilteredWriteOperationStream = new WriteEventStreamReader(writeOperationsFileReader, EventReturnPolicy.AT_LEAST_ONE_MATCH);

        /*
         * Filter Write Operations
         */
        Predicate<Operation<?>> enabledWriteOperationsFilter = new Predicate<Operation<?>>() {
            @Override
            public boolean apply(Operation<?> operation) {
                return writeOperationFilter.contains(operation.getClass());
            }
        };
        Iterator<Operation<?>> filteredWriteOperationStream = Iterators.filter(unfilteredWriteOperationStream, enabledWriteOperationsFilter);

        /*
         * Move write operations to same start time as read operations
         */
        // TODO add parameter, or do in more intelligent way
        Duration firstWriteOperationFromWorkloadStart = Duration.fromSeconds(1);
        Iterator<Operation<?>> offsetFilteredWriteOperationStream = gf.timeOffset(
                // assign place holder dependency times so time offset function does not complain about null values
                gf.assignDependencyTimes(
                        gf.constant(Time.fromMilli(0)),
                        filteredWriteOperationStream
                ),
                workloadStartTime.plus(firstWriteOperationFromWorkloadStart)
        );

        /*
         * Add Dependency Times To Dependent Write Operations
         */
        Function1<Operation<?>, Boolean> isDependency = new Function1<Operation<?>, Boolean>() {
            @Override
            public Boolean apply(Operation<?> operation) {
                OperationClassification.DependencyMode operationDependencyMode = operationClassifications.get(operation.getClass()).dependencyMode();
                // TODO add GctMode.WRITE in future?
                return operationDependencyMode.equals(OperationClassification.DependencyMode.READ_WRITE);
            }
        };
        boolean canOverwriteDependencyTime = true;
        Iterator<Operation<?>> offsetFilteredWriteOperationStreamWithDependencyTimes = gf.assignDependencyTimesEqualToLastEncounteredLowerDependencyStartTime(
                offsetFilteredWriteOperationStream,
                isDependency,
                workloadStartTime,
                canOverwriteDependencyTime);


        List<Iterator<Operation<?>>> streamsOfAllEnabledOperationTypes = new ArrayList<>();
        if (false == writeOperationFilter.isEmpty())
            streamsOfAllEnabledOperationTypes.add(offsetFilteredWriteOperationStreamWithDependencyTimes);
        if (readOperationFilter.contains(LdbcQuery1.class))
            streamsOfAllEnabledOperationTypes.add(readOperation1Stream);
        if (readOperationFilter.contains(LdbcQuery2.class))
            streamsOfAllEnabledOperationTypes.add(readOperation2Stream);
        if (readOperationFilter.contains(LdbcQuery3.class))
            streamsOfAllEnabledOperationTypes.add(readOperation3Stream);
        if (readOperationFilter.contains(LdbcQuery4.class))
            streamsOfAllEnabledOperationTypes.add(readOperation4Stream);
        if (readOperationFilter.contains(LdbcQuery5.class))
            streamsOfAllEnabledOperationTypes.add(readOperation5Stream);
        if (readOperationFilter.contains(LdbcQuery6.class))
            streamsOfAllEnabledOperationTypes.add(readOperation6Stream);
        if (readOperationFilter.contains(LdbcQuery7.class))
            streamsOfAllEnabledOperationTypes.add(readOperation7Stream);
        if (readOperationFilter.contains(LdbcQuery8.class))
            streamsOfAllEnabledOperationTypes.add(readOperation8Stream);
        if (readOperationFilter.contains(LdbcQuery9.class))
            streamsOfAllEnabledOperationTypes.add(readOperation9Stream);
        if (readOperationFilter.contains(LdbcQuery10.class))
            streamsOfAllEnabledOperationTypes.add(readOperation10Stream);
        if (readOperationFilter.contains(LdbcQuery11.class))
            streamsOfAllEnabledOperationTypes.add(readOperation11Stream);
        if (readOperationFilter.contains(LdbcQuery12.class))
            streamsOfAllEnabledOperationTypes.add(readOperation12Stream);
        if (readOperationFilter.contains(LdbcQuery13.class))
            streamsOfAllEnabledOperationTypes.add(readOperation13Stream);
        if (readOperationFilter.contains(LdbcQuery14.class))
            streamsOfAllEnabledOperationTypes.add(readOperation14Stream);

        /*
         * Merge all operation streams, ordered by operation start times
         */
        Iterator<Operation<?>> readAndWriteOperations = gf.mergeSortOperationsByStartTime(
                streamsOfAllEnabledOperationTypes.toArray(new Iterator[streamsOfAllEnabledOperationTypes.size()])
        );

        return readAndWriteOperations;

        // TODO alternatively, most aggressive, i.e., all operations have their dependencies met before starting
//        Iterator<Time> dependencyTimes = gf.constantIncrementTime(workloadStartTime, Duration.fromMilli(0));
//        return gf.assignDependencyTimes(dependencyTimes, readAndWriteOperations);
    }

    @Override
    public DbValidationParametersFilter dbValidationParametersFilter(Integer requiredValidationParameterCount) {
        /**
         * TODO
         * operationTypeCount = 14
         * requiredValidationParameterCount = 100
         * 100/14 == 7.14
         * --> minimumResultCountPerOperationType = 7
         * 100%14 == 2 > 0
         * --> minimumResultCountPerOperationType = 8
         *
         */
        Integer operationTypeCount = readOperationFilter.size();
        long minimumResultCountPerOperationType = Math.max(1, Math.round(Math.floor(requiredValidationParameterCount.doubleValue() / operationTypeCount.doubleValue())));

//        if (requiredValidationParameterCount % operationTypeCount > 0)
//            minimumResultCountPerOperationType++;

        final Map<Class, Long> remainingRequiredResultsPerOperationType = new HashMap<>();
        long resultCountsAssignedSoFar = 0;
        for (Class operationType : readOperationFilter) {
            remainingRequiredResultsPerOperationType.put(operationType, minimumResultCountPerOperationType);
            resultCountsAssignedSoFar = resultCountsAssignedSoFar + minimumResultCountPerOperationType;
        }
        for (Class operationType : remainingRequiredResultsPerOperationType.keySet()) {
            if (resultCountsAssignedSoFar >= requiredValidationParameterCount)
                break;
            remainingRequiredResultsPerOperationType.put(operationType, remainingRequiredResultsPerOperationType.get(operationType) + 1);
            resultCountsAssignedSoFar++;
        }

        return new DbValidationParametersFilter() {
            @Override
            public boolean useOperation(Operation<?> operation) {
                Class operationType = operation.getClass();

                boolean isNotReadOperation = false == readOperationFilter.contains(operationType);
                if (isNotReadOperation) return false;

                boolean alreadyHaveAllRequiredResultsForOperationType = false == remainingRequiredResultsPerOperationType.containsKey(operationType);
                if (alreadyHaveAllRequiredResultsForOperationType) return false;

                return true;
            }

            @Override
            public DbValidationParametersFilterResult useOperationAndResultForValidation(Operation<?> operation, Object operationResult) {
                Class operationType = operation.getClass();

                boolean isEmptyResult = ((List) operationResult).isEmpty();
                if (isEmptyResult) return DbValidationParametersFilterResult.REJECT_AND_CONTINUE;

                long remainingRequiredResultsForOperationType = remainingRequiredResultsPerOperationType.get(operationType) - 1;

                if (0 == remainingRequiredResultsForOperationType)
                    remainingRequiredResultsPerOperationType.remove(operationType);
                else
                    remainingRequiredResultsPerOperationType.put(operationType, remainingRequiredResultsForOperationType);

                if (remainingRequiredResultsPerOperationType.size() > 0)
                    return DbValidationParametersFilterResult.ACCEPT_AND_CONTINUE;
                else
                    return DbValidationParametersFilterResult.ACCEPT_AND_FINISH;
            }
        };
    }

    @Override
    public Duration maxExpectedInterleave() {
        return Duration.fromMinutes(30);
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference TYPE_REFERENCE = new TypeReference<List<Object>>() {
    };

    @Override
    public String serializeOperation(Operation<?> operation) throws SerializingMarshallingException {
        if (operation.getClass().equals(LdbcQuery1.class)) {
            LdbcQuery1 ldbcQuery = (LdbcQuery1) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.personUri());
            operationAsList.add(ldbcQuery.firstName());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery2.class)) {
            LdbcQuery2 ldbcQuery = (LdbcQuery2) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.personUri());
            operationAsList.add(ldbcQuery.maxDate().getTime());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery3.class)) {
            LdbcQuery3 ldbcQuery = (LdbcQuery3) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.personUri());
            operationAsList.add(ldbcQuery.countryXName());
            operationAsList.add(ldbcQuery.countryYName());
            operationAsList.add(ldbcQuery.startDate().getTime());
            operationAsList.add(ldbcQuery.durationDays());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery4.class)) {
            LdbcQuery4 ldbcQuery = (LdbcQuery4) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.personUri());
            operationAsList.add(ldbcQuery.startDate().getTime());
            operationAsList.add(ldbcQuery.durationDays());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery5.class)) {
            LdbcQuery5 ldbcQuery = (LdbcQuery5) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.personUri());
            operationAsList.add(ldbcQuery.minDate().getTime());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery6.class)) {
            LdbcQuery6 ldbcQuery = (LdbcQuery6) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.personUri());
            operationAsList.add(ldbcQuery.tagName());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery7.class)) {
            LdbcQuery7 ldbcQuery = (LdbcQuery7) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.personUri());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery8.class)) {
            LdbcQuery8 ldbcQuery = (LdbcQuery8) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.personUri());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery9.class)) {
            LdbcQuery9 ldbcQuery = (LdbcQuery9) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.personUri());
            operationAsList.add(ldbcQuery.maxDate().getTime());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery10.class)) {
            LdbcQuery10 ldbcQuery = (LdbcQuery10) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.personUri());
            operationAsList.add(ldbcQuery.month());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery11.class)) {
            LdbcQuery11 ldbcQuery = (LdbcQuery11) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.personUri());
            operationAsList.add(ldbcQuery.countryName());
            operationAsList.add(ldbcQuery.workFromYear());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery12.class)) {
            LdbcQuery12 ldbcQuery = (LdbcQuery12) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.personUri());
            operationAsList.add(ldbcQuery.tagClassName());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery13.class)) {
            LdbcQuery13 ldbcQuery = (LdbcQuery13) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.person1Id());
            operationAsList.add(ldbcQuery.person1Uri());
            operationAsList.add(ldbcQuery.person2Id());
            operationAsList.add(ldbcQuery.person2Uri());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery14.class)) {
            LdbcQuery14 ldbcQuery = (LdbcQuery14) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.person1Id());
            operationAsList.add(ldbcQuery.person1Uri());
            operationAsList.add(ldbcQuery.person2Id());
            operationAsList.add(ldbcQuery.person2Uri());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcUpdate1AddPerson.class)) {
            LdbcUpdate1AddPerson ldbcQuery = (LdbcUpdate1AddPerson) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.personFirstName());
            operationAsList.add(ldbcQuery.personLastName());
            operationAsList.add(ldbcQuery.gender());
            operationAsList.add(ldbcQuery.birthday().getTime());
            operationAsList.add(ldbcQuery.creationDate().getTime());
            operationAsList.add(ldbcQuery.locationIp());
            operationAsList.add(ldbcQuery.browserUsed());
            operationAsList.add(ldbcQuery.cityId());
            operationAsList.add(ldbcQuery.languages());
            operationAsList.add(ldbcQuery.emails());
            operationAsList.add(ldbcQuery.tagIds());
            Iterable<Map<String, Object>> studyAt = Lists.newArrayList(Iterables.transform(ldbcQuery.studyAt(), new Function<LdbcUpdate1AddPerson.Organization, Map<String, Object>>() {
                @Override
                public Map<String, Object> apply(LdbcUpdate1AddPerson.Organization organization) {
                    Map<String, Object> organizationMap = new HashMap<>();
                    organizationMap.put("id", organization.organizationId());
                    organizationMap.put("year", organization.year());
                    return organizationMap;
                }
            }));
            operationAsList.add(studyAt);
            Iterable<Map<String, Object>> workAt = Lists.newArrayList(Iterables.transform(ldbcQuery.workAt(), new Function<LdbcUpdate1AddPerson.Organization, Map<String, Object>>() {
                @Override
                public Map<String, Object> apply(LdbcUpdate1AddPerson.Organization organization) {
                    Map<String, Object> organizationMap = new HashMap<>();
                    organizationMap.put("id", organization.organizationId());
                    organizationMap.put("year", organization.year());
                    return organizationMap;
                }
            }));
            operationAsList.add(workAt);
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcUpdate2AddPostLike.class)) {
            LdbcUpdate2AddPostLike ldbcQuery = (LdbcUpdate2AddPostLike) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.postId());
            operationAsList.add(ldbcQuery.creationDate().getTime());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcUpdate3AddCommentLike.class)) {
            LdbcUpdate3AddCommentLike ldbcQuery = (LdbcUpdate3AddCommentLike) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.commentId());
            operationAsList.add(ldbcQuery.creationDate().getTime());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcUpdate4AddForum.class)) {
            LdbcUpdate4AddForum ldbcQuery = (LdbcUpdate4AddForum) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.forumId());
            operationAsList.add(ldbcQuery.forumTitle());
            operationAsList.add(ldbcQuery.creationDate().getTime());
            operationAsList.add(ldbcQuery.moderatorPersonId());
            operationAsList.add(ldbcQuery.tagIds());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcUpdate5AddForumMembership.class)) {
            LdbcUpdate5AddForumMembership ldbcQuery = (LdbcUpdate5AddForumMembership) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.forumId());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.creationDate().getTime());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcUpdate6AddPost.class)) {
            LdbcUpdate6AddPost ldbcQuery = (LdbcUpdate6AddPost) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.postId());
            operationAsList.add(ldbcQuery.imageFile());
            operationAsList.add(ldbcQuery.creationDate().getTime());
            operationAsList.add(ldbcQuery.locationIp());
            operationAsList.add(ldbcQuery.browserUsed());
            operationAsList.add(ldbcQuery.language());
            operationAsList.add(ldbcQuery.content());
            operationAsList.add(ldbcQuery.length());
            operationAsList.add(ldbcQuery.authorPersonId());
            operationAsList.add(ldbcQuery.forumId());
            operationAsList.add(ldbcQuery.countryId());
            operationAsList.add(ldbcQuery.tagIds());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcUpdate7AddComment.class)) {
            LdbcUpdate7AddComment ldbcQuery = (LdbcUpdate7AddComment) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.commentId());
            operationAsList.add(ldbcQuery.creationDate());
            operationAsList.add(ldbcQuery.locationIp());
            operationAsList.add(ldbcQuery.browserUsed());
            operationAsList.add(ldbcQuery.content());
            operationAsList.add(ldbcQuery.length());
            operationAsList.add(ldbcQuery.authorPersonId());
            operationAsList.add(ldbcQuery.countryId());
            operationAsList.add(ldbcQuery.replyToPostId());
            operationAsList.add(ldbcQuery.replyToCommentId());
            operationAsList.add(ldbcQuery.tagIds());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcUpdate8AddFriendship.class)) {
            LdbcUpdate8AddFriendship ldbcQuery = (LdbcUpdate8AddFriendship) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.person1Id());
            operationAsList.add(ldbcQuery.person2Id());
            operationAsList.add(ldbcQuery.creationDate().getTime());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        throw new SerializingMarshallingException(
                String.format("Workload does not know how to serialize operation\nWorkload: %s\nOperation Type: %s\nOperation: %s",
                        getClass().getName(),
                        operation.getClass().getName(),
                        operation));
    }

    @Override
    public Operation<?> marshalOperation(String serializedOperation) throws SerializingMarshallingException {
        List<Object> operationAsList;
        try {
            operationAsList = OBJECT_MAPPER.readValue(serializedOperation, TYPE_REFERENCE);
        } catch (IOException e) {
            throw new SerializingMarshallingException(String.format("Error while parsing serialized results\n%s", serializedOperation), e);
        }

        if (operationAsList.get(0).equals(LdbcQuery1.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            String firstName = (String) operationAsList.get(3);
            int limit = ((Number) operationAsList.get(4)).intValue();
            return new LdbcQuery1(personId, personUri, firstName, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery2.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            Date maxDate = new Date(((Number) operationAsList.get(3)).longValue());
            int limit = ((Number) operationAsList.get(4)).intValue();
            return new LdbcQuery2(personId, personUri, maxDate, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery3.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            String countryXName = (String) operationAsList.get(3);
            String countryYName = (String) operationAsList.get(4);
            Date startDate = new Date(((Number) operationAsList.get(5)).longValue());
            int durationDays = ((Number) operationAsList.get(6)).intValue();
            int limit = ((Number) operationAsList.get(7)).intValue();
            return new LdbcQuery3(personId, personUri, countryXName, countryYName, startDate, durationDays, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery4.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            Date startDate = new Date(((Number) operationAsList.get(3)).longValue());
            int durationDays = ((Number) operationAsList.get(4)).intValue();
            int limit = ((Number) operationAsList.get(5)).intValue();
            return new LdbcQuery4(personId, personUri, startDate, durationDays, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery5.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            Date minDate = new Date(((Number) operationAsList.get(3)).longValue());
            int limit = ((Number) operationAsList.get(4)).intValue();
            return new LdbcQuery5(personId, personUri, minDate, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery6.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            String tagName = (String) operationAsList.get(3);
            int limit = ((Number) operationAsList.get(4)).intValue();
            return new LdbcQuery6(personId, personUri, tagName, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery7.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            int limit = ((Number) operationAsList.get(3)).intValue();
            return new LdbcQuery7(personId, personUri, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery8.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            int limit = ((Number) operationAsList.get(3)).intValue();
            return new LdbcQuery8(personId, personUri, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery9.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            Date maxDate = new Date(((Number) operationAsList.get(3)).longValue());
            int limit = ((Number) operationAsList.get(4)).intValue();
            return new LdbcQuery9(personId, personUri, maxDate, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery10.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            int month = ((Number) operationAsList.get(3)).intValue();
            int limit = ((Number) operationAsList.get(4)).intValue();
            return new LdbcQuery10(personId, personUri, month, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery11.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            String countryName = (String) operationAsList.get(3);
            int workFromYear = ((Number) operationAsList.get(4)).intValue();
            int limit = ((Number) operationAsList.get(5)).intValue();
            return new LdbcQuery11(personId, personUri, countryName, workFromYear, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery12.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            String tagClassName = (String) operationAsList.get(3);
            int limit = ((Number) operationAsList.get(4)).intValue();
            return new LdbcQuery12(personId, personUri, tagClassName, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery13.class.getName())) {
            long person1Id = ((Number) operationAsList.get(1)).longValue();
            String person1Uri = (String) operationAsList.get(2);
            long person2Id = ((Number) operationAsList.get(3)).longValue();
            String person2Uri = (String) operationAsList.get(4);
            return new LdbcQuery13(person1Id, person1Uri, person2Id, person2Uri);
        }

        if (operationAsList.get(0).equals(LdbcQuery14.class.getName())) {
            long person1Id = ((Number) operationAsList.get(1)).longValue();
            String person1Uri = (String) operationAsList.get(2);
            long person2Id = ((Number) operationAsList.get(3)).longValue();
            String person2Uri = (String) operationAsList.get(4);
            return new LdbcQuery14(person1Id, person1Uri, person2Id, person2Uri);
        }

        if (operationAsList.get(0).equals(LdbcUpdate1AddPerson.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personFirstName = (String) operationAsList.get(2);
            String personLastName = (String) operationAsList.get(3);
            String gender = (String) operationAsList.get(4);
            Date birthday = new Date(((Number) operationAsList.get(5)).longValue());
            Date creationDate = new Date(((Number) operationAsList.get(6)).longValue());
            String locationIp = (String) operationAsList.get(7);
            String browserUsed = (String) operationAsList.get(8);
            long cityId = ((Number) operationAsList.get(9)).longValue();
            List<String> languages = (List<String>) operationAsList.get(10);
            List<String> emails = (List<String>) operationAsList.get(11);
            List<Long> tagIds = Lists.newArrayList(Iterables.transform((List<Number>) operationAsList.get(12), new Function<Number, Long>() {
                @Override
                public Long apply(Number number) {
                    return number.longValue();
                }
            }));
            List<Map<String, Object>> studyAtList = (List<Map<String, Object>>) operationAsList.get(13);
            List<LdbcUpdate1AddPerson.Organization> studyAt = Lists.newArrayList(Iterables.transform(studyAtList, new Function<Map<String, Object>, LdbcUpdate1AddPerson.Organization>() {
                @Override
                public LdbcUpdate1AddPerson.Organization apply(Map<String, Object> input) {
                    long organizationId = ((Number) input.get("id")).longValue();
                    int year = ((Number) input.get("year")).intValue();
                    return new LdbcUpdate1AddPerson.Organization(organizationId, year);
                }
            }));
            List<Map<String, Object>> workAtList = (List<Map<String, Object>>) operationAsList.get(14);
            List<LdbcUpdate1AddPerson.Organization> workAt = Lists.newArrayList(Iterables.transform(workAtList, new Function<Map<String, Object>, LdbcUpdate1AddPerson.Organization>() {
                @Override
                public LdbcUpdate1AddPerson.Organization apply(Map<String, Object> input) {
                    long organizationId = ((Number) input.get("id")).longValue();
                    int year = ((Number) input.get("year")).intValue();
                    return new LdbcUpdate1AddPerson.Organization(organizationId, year);
                }
            }));

            return new LdbcUpdate1AddPerson(personId, personFirstName, personLastName, gender, birthday, creationDate,
                    locationIp, browserUsed, cityId, languages, emails, tagIds, studyAt, workAt);
        }

        if (operationAsList.get(0).equals(LdbcUpdate2AddPostLike.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            long postId = ((Number) operationAsList.get(2)).longValue();
            Date creationDate = new Date(((Number) operationAsList.get(3)).longValue());

            return new LdbcUpdate2AddPostLike(personId, postId, creationDate);
        }

        if (operationAsList.get(0).equals(LdbcUpdate3AddCommentLike.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            long commentId = ((Number) operationAsList.get(2)).longValue();
            Date creationDate = new Date(((Number) operationAsList.get(3)).longValue());

            return new LdbcUpdate3AddCommentLike(personId, commentId, creationDate);
        }

        if (operationAsList.get(0).equals(LdbcUpdate4AddForum.class.getName())) {
            long forumId = ((Number) operationAsList.get(1)).longValue();
            String forumTitle = (String) operationAsList.get(2);
            Date creationDate = new Date(((Number) operationAsList.get(3)).longValue());
            long moderatorPersonId = ((Number) operationAsList.get(4)).longValue();
            List<Long> tagIds = Lists.newArrayList(Iterables.transform((List<Number>) operationAsList.get(5), new Function<Number, Long>() {
                @Override
                public Long apply(Number number) {
                    return number.longValue();
                }
            }));

            return new LdbcUpdate4AddForum(forumId, forumTitle, creationDate, moderatorPersonId, tagIds);
        }


        if (operationAsList.get(0).equals(LdbcUpdate5AddForumMembership.class.getName())) {
            long forumId = ((Number) operationAsList.get(1)).longValue();
            long personId = ((Number) operationAsList.get(2)).longValue();
            Date creationDate = new Date(((Number) operationAsList.get(3)).longValue());

            return new LdbcUpdate5AddForumMembership(forumId, personId, creationDate);
        }

        if (operationAsList.get(0).equals(LdbcUpdate6AddPost.class.getName())) {
            long postId = ((Number) operationAsList.get(1)).longValue();
            String imageFile = (String) operationAsList.get(2);
            Date creationDate = new Date(((Number) operationAsList.get(3)).longValue());
            String locationIp = (String) operationAsList.get(4);
            String browserUsed = (String) operationAsList.get(5);
            String language = (String) operationAsList.get(6);
            String content = (String) operationAsList.get(7);
            int length = ((Number) operationAsList.get(8)).intValue();
            long authorPersonId = ((Number) operationAsList.get(9)).longValue();
            long forumId = ((Number) operationAsList.get(10)).longValue();
            long countryId = ((Number) operationAsList.get(11)).longValue();
            List<Long> tagIds = Lists.newArrayList(Iterables.transform((List<Number>) operationAsList.get(12), new Function<Number, Long>() {
                @Override
                public Long apply(Number number) {
                    return number.longValue();
                }
            }));

            return new LdbcUpdate6AddPost(postId, imageFile, creationDate, locationIp, browserUsed, language, content, length, authorPersonId, forumId, countryId, tagIds);
        }

        if (operationAsList.get(0).equals(LdbcUpdate7AddComment.class.getName())) {
            long commentId = ((Number) operationAsList.get(1)).longValue();
            Date creationDate = new Date(((Number) operationAsList.get(2)).longValue());
            String locationIp = (String) operationAsList.get(3);
            String browserUsed = (String) operationAsList.get(4);
            String content = (String) operationAsList.get(5);
            int length = ((Number) operationAsList.get(6)).intValue();
            long authorPersonId = ((Number) operationAsList.get(7)).longValue();
            long countryId = ((Number) operationAsList.get(8)).longValue();
            long replyToPostId = ((Number) operationAsList.get(9)).longValue();
            long replyToCommentId = ((Number) operationAsList.get(10)).longValue();
            List<Long> tagIds = Lists.newArrayList(Iterables.transform((List<Number>) operationAsList.get(11), new Function<Number, Long>() {
                @Override
                public Long apply(Number number) {
                    return number.longValue();
                }
            }));

            return new LdbcUpdate7AddComment(commentId, creationDate, locationIp, browserUsed, content, length, authorPersonId, countryId, replyToPostId, replyToCommentId, tagIds);
        }

        if (operationAsList.get(0).equals(LdbcUpdate8AddFriendship.class.getName())) {
            long person1Id = ((Number) operationAsList.get(1)).longValue();
            long person2Id = ((Number) operationAsList.get(2)).longValue();
            Date creationDate = new Date(((Number) operationAsList.get(3)).longValue());

            return new LdbcUpdate8AddFriendship(person1Id, person2Id, creationDate);
        }

        throw new SerializingMarshallingException(
                String.format("Workload does not know how to marshal operation\nWorkload: %s\nAssumed Operation Type: %s\nSerialized Operation: %s",
                        getClass().getName(),
                        operationAsList.get(0),
                        serializedOperation));
    }
}
