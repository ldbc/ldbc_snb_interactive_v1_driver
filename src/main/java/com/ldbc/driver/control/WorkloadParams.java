package com.ldbc.driver.control;

import com.ldbc.driver.Client;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.util.MapUtils;
import org.apache.commons.cli.*;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

public class WorkloadParams implements DriverConfiguration {

    // --- REQUIRED ---
    public static final String OPERATION_COUNT_ARG = "oc";
    private static final String OPERATION_COUNT_ARG_LONG = "operationcount";
    private static final String OPERATION_COUNT_DEFAULT = Integer.toString(0);
    private static final String OPERATION_COUNT_DESCRIPTION = String.format(
            "number of operations to execute (default: %s)", OPERATION_COUNT_DEFAULT);
    public static final long UNBOUNDED_OPERATION_COUNT = -1;

    public static final String WORKLOAD_ARG = "w";
    private static final String WORKLOAD_ARG_LONG = "workload";
    private static final String WORKLOAD_EXAMPLE = com.ldbc.driver.workloads.simple.SimpleWorkload.class.getName();
    private static final String WORKLOAD_DESCRIPTION = String.format("classname of the Workload to use (e.g. %s)",
            WORKLOAD_EXAMPLE);

    public static final String DB_ARG = "db";
    private static final String DB_ARG_LONG = "database";
    private static final String DB_EXAMPLE = com.ldbc.driver.workloads.simple.db.BasicDb.class.getName();
    private static final String DB_DESCRIPTION = String.format("classname of the DB to use (e.g. %s)", DB_EXAMPLE);

    // --- OPTIONAL ---
    public static final String RESULT_FILE_PATH_ARG = "rf";
    private static final String RESULT_FILE_PATH_ARG_LONG = "resultfile";
    private static final String RESULT_FILE_PATH_DESCRIPTION =
            "where benchmark results JSON file will be written (null = file will not be created)";

    public static final String THREADS_ARG = "tc";
    private static final String THREADS_ARG_LONG = "threadcount";
    private static final String THREADS_DEFAULT = Integer.toString(calculateDefaultThreadPoolSize());
    private static final String THREADS_DESCRIPTION = String.format(
            "number of worker threads to execute with (default: %s)", THREADS_DEFAULT);

    public static int calculateDefaultThreadPoolSize() {
        // Client & OperationResultLoggingThread
        int threadsUsedByDriver = 2;
        int totalProcessors = Runtime.getRuntime().availableProcessors();
        int availableProcessors = totalProcessors - threadsUsedByDriver;
        return Math.max(1, availableProcessors);
    }

    //TODO Duration.fromSeconds(2), make status an integer argument and 0==no status
    public static final String SHOW_STATUS_ARG = "s";
    private static final String SHOW_STATUS_ARG_LONG = "status";
    private static final String SHOW_STATUS_DEFAULT = Boolean.toString(false);
    private static final String SHOW_STATUS_DESCRIPTION = "show status during run";

    public static final String PROPERTY_FILE_ARG = "P";
    private static final String PROPERTY_FILE_DESCRIPTION = "load properties from file(s) - files will be loaded in the order provided\n" +
            "first files are highest priority; later values will not override earlier values";

    public static final String PROPERTY_ARG = "p";
    private static final String PROPERTY_DESCRIPTION = "properties to be passed to DB and Workload - these will override properties loaded from files";

    public static final String TIME_UNIT_ARG = "tu";
    private static final String TIME_UNIT_ARG_LONG = "timeunit";
    private static final String TIME_UNIT_DEFAULT = TimeUnit.MILLISECONDS.toString();
    private static final TimeUnit[] VALID_TIME_UNITS = new TimeUnit[]{TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS,
            TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES};
    private static final String TIME_UNIT_DESCRIPTION = String.format(
            "time unit to use when gathering metrics. default:%s, valid:%s", TIME_UNIT_DEFAULT,
            Arrays.toString(VALID_TIME_UNITS));

    public static final String TIME_COMPRESSION_RATIO_ARG = "tcr";
    private static final String TIME_COMPRESSION_RATIO_ARG_LONG = "timecompressionratio";
    private static final String TIME_COMPRESSION_RATIO_DEFAULT = "1"; // 1 == do not compress
    private static final String TIME_COMPRESSION_RATIO_DESCRIPTION = "change duration between operations of workload";

    public static final String GCT_DELTA_DURATION_ARG = "gctd";
    private static final String GCT_DELTA_DURATION_ARG_LONG = "gctdeltaduration";
    private static final String GCT_DELTA_DURATION_DEFAULT = Duration.fromSeconds(3600).asMilli().toString();
    private static final String GCT_DELTA_DURATION_DESCRIPTION = "safe duration (ms) between dependent operations";

    public static final String PEER_IDS_ARG = "pids";
    private static final String PEER_IDS_ARG_LONG = "peeridentifiers";
    private static final String PEER_IDS_DEFAULT = "[]";
    private static final String PEER_IDS_DESCRIPTION = "identifiers/addresses of other driver workers (for distributed mode)";

    public static final String TOLERATED_EXECUTION_DELAY_ARG = "del";
    private static final String TOLERATED_EXECUTION_DELAY_ARG_LONG = "toleratedexecutiondelay";
    private static final String TOLERATED_EXECUTION_DELAY_DEFAULT = Duration.fromSeconds(1).asMilli().toString();
    private static final String TOLERATED_EXECUTION_DELAY_DESCRIPTION = "duration (ms) an operation handler may miss its scheduled start time by";

    private static final Options OPTIONS = buildOptions();

    public static WorkloadParams fromArgs(String[] args) throws ParamsException {
        Map<String, String> paramsMap;
        try {
            paramsMap = parseArgs(args, OPTIONS);
            assertRequiredArgsProvided(paramsMap);
            assertValidTimeUnit(paramsMap.get(TIME_UNIT_ARG));
            paramsMap = MapUtils.mergeMaps(paramsMap, defaultParamValues(), false);
        } catch (ParseException e) {
            throw new ParamsException(e.getMessage());
        } catch (ParamsException e) {
            throw new ParamsException(String.format("%s\n%s", e.getMessage(), helpString()));
        }

        String dbClassName = paramsMap.get(DB_ARG);
        String workloadClassName = paramsMap.get(WORKLOAD_ARG);
        long operationCount = Long.parseLong(paramsMap.get(OPERATION_COUNT_ARG));
        int threadCount = Integer.parseInt(paramsMap.get(THREADS_ARG));
        boolean showStatus = Boolean.parseBoolean(paramsMap.get(SHOW_STATUS_ARG));
        TimeUnit timeUnit = TimeUnit.valueOf(paramsMap.get(TIME_UNIT_ARG));
        String resultFilePath = paramsMap.get(RESULT_FILE_PATH_ARG);
        Double timeCompressionRatio = Double.parseDouble(paramsMap.get(TIME_COMPRESSION_RATIO_ARG));
        Duration gctDeltaDuration = Duration.fromMilli(Long.parseLong(paramsMap.get(GCT_DELTA_DURATION_ARG)));
        List<String> peerIds = parsePeerIds(paramsMap.get(PEER_IDS_ARG));
        Duration toleratedExecutionDelay = Duration.fromMilli(Long.parseLong(paramsMap.get(TOLERATED_EXECUTION_DELAY_ARG)));
        return new WorkloadParams(paramsMap, dbClassName, workloadClassName, operationCount,
                threadCount, showStatus, timeUnit, resultFilePath, timeCompressionRatio, gctDeltaDuration, peerIds, toleratedExecutionDelay);
    }

    private static void assertRequiredArgsProvided(Map<String, String> paramsMap) throws ParamsException {
        List<String> missingOptions = new ArrayList<String>();
        if (null == paramsMap.get(DB_ARG)) missingOptions.add(DB_ARG);
        if (null == paramsMap.get(WORKLOAD_ARG)) missingOptions.add(WORKLOAD_ARG);
        if (null == paramsMap.get(OPERATION_COUNT_ARG)) missingOptions.add(OPERATION_COUNT_ARG);
        if (false == missingOptions.isEmpty())
            throw new ParamsException(String.format("Missing required option: %s", missingOptions.toString()));
    }

    private static void assertValidTimeUnit(String timeUnitString) throws ParamsException {
        try {
            TimeUnit timeUnit = TimeUnit.valueOf(timeUnitString);
            Set<TimeUnit> validTimeUnits = new HashSet<TimeUnit>();
            validTimeUnits.addAll(Arrays.asList(VALID_TIME_UNITS));
            if (false == validTimeUnits.contains(timeUnit)) {
                throw new IllegalArgumentException();
            }
        } catch (IllegalArgumentException e) {
            throw new ParamsException(String.format("Unsupported TimeUnit value: %s", timeUnitString));
        }
    }

    private static Map<String, String> defaultParamValues() {
        Map<String, String> defaultParamValues = new HashMap<String, String>();
        defaultParamValues.put(THREADS_ARG, THREADS_DEFAULT);
        defaultParamValues.put(SHOW_STATUS_ARG, SHOW_STATUS_DEFAULT);
        defaultParamValues.put(TIME_UNIT_ARG, TIME_UNIT_DEFAULT);
        defaultParamValues.put(TIME_COMPRESSION_RATIO_ARG, TIME_COMPRESSION_RATIO_DEFAULT);
        defaultParamValues.put(GCT_DELTA_DURATION_ARG, GCT_DELTA_DURATION_DEFAULT);
        defaultParamValues.put(PEER_IDS_ARG, PEER_IDS_DEFAULT);
        defaultParamValues.put(TOLERATED_EXECUTION_DELAY_ARG, TOLERATED_EXECUTION_DELAY_DEFAULT);
        return defaultParamValues;
    }

    private static Map<String, String> parseArgs(String[] args, Options options) throws ParseException, ParamsException {
        Map<String, String> cmdParams = new HashMap<String, String>();
        Map<String, String> fileParams = new HashMap<String, String>();

        CommandLineParser parser = new BasicParser();

        CommandLine cmd = parser.parse(options, args);

        /*
         * Required
         */
        if (cmd.hasOption(DB_ARG))
            cmdParams.put(DB_ARG, cmd.getOptionValue(DB_ARG));

        if (cmd.hasOption(WORKLOAD_ARG))
            cmdParams.put(WORKLOAD_ARG, cmd.getOptionValue(WORKLOAD_ARG));

        if (cmd.hasOption(OPERATION_COUNT_ARG))
            cmdParams.put(OPERATION_COUNT_ARG, cmd.getOptionValue(OPERATION_COUNT_ARG));

        /*
         * Optional
         */
        if (cmd.hasOption(RESULT_FILE_PATH_ARG))
            cmdParams.put(RESULT_FILE_PATH_ARG, cmd.getOptionValue(RESULT_FILE_PATH_ARG));

        if (cmd.hasOption(THREADS_ARG))
            cmdParams.put(THREADS_ARG, cmd.getOptionValue(THREADS_ARG));

        if (cmd.hasOption(SHOW_STATUS_ARG))
            cmdParams.put(SHOW_STATUS_ARG, Boolean.toString(true));

        if (cmd.hasOption(TIME_UNIT_ARG))
            cmdParams.put(TIME_UNIT_ARG, cmd.getOptionValue(TIME_UNIT_ARG));

        if (cmd.hasOption(TIME_COMPRESSION_RATIO_ARG))
            cmdParams.put(TIME_COMPRESSION_RATIO_ARG, cmd.getOptionValue(TIME_COMPRESSION_RATIO_ARG));

        if (cmd.hasOption(GCT_DELTA_DURATION_ARG))
            cmdParams.put(GCT_DELTA_DURATION_ARG, cmd.getOptionValue(GCT_DELTA_DURATION_ARG));

        if (cmd.hasOption(TOLERATED_EXECUTION_DELAY_ARG))
            cmdParams.put(TOLERATED_EXECUTION_DELAY_ARG, cmd.getOptionValue(TOLERATED_EXECUTION_DELAY_ARG));

        if (cmd.hasOption(PEER_IDS_ARG)) {
            List<String> peerIds = new ArrayList<String>();
            for (String peerId : cmd.getOptionValues(PEER_IDS_ARG)) {
                peerIds.add(peerId);
            }
            cmdParams.put(PEER_IDS_ARG, serializePeerIds(peerIds));
        }

        if (cmd.hasOption(PROPERTY_FILE_ARG)) {
            for (String propertyFilePath : cmd.getOptionValues(PROPERTY_FILE_ARG)) {
                // code assumes ordering -> first files more important than last, first values get priority
                try {
                    Properties tempFileProperties = new Properties();
                    tempFileProperties.load(new FileInputStream(propertyFilePath));
                    Map<String, String> tempFileParams = MapUtils.propertiesToMap(tempFileProperties);
                    boolean overwrite = true;
                    fileParams = MapUtils.mergeMaps(convertLongKeysToShortKeys(tempFileParams), fileParams, overwrite);
                } catch (IOException e) {
                    throw new ParseException(String.format("Error loading properties file %s\n%s", propertyFilePath, e.getMessage()));
                }
            }
        }

        if (cmd.hasOption(PROPERTY_ARG)) {
            for (Entry<Object, Object> cmdProperty : cmd.getOptionProperties(PROPERTY_ARG).entrySet()) {
                cmdParams.put((String) cmdProperty.getKey(), (String) cmdProperty.getValue());
            }
        }

        boolean overwrite = true;
        return MapUtils.mergeMaps(convertLongKeysToShortKeys(fileParams), convertLongKeysToShortKeys(cmdParams), overwrite);
    }

    private static Map<String, String> convertLongKeysToShortKeys(Map<String, String> paramsMap) {
        paramsMap = replaceKey(paramsMap, OPERATION_COUNT_ARG_LONG, OPERATION_COUNT_ARG);
        paramsMap = replaceKey(paramsMap, WORKLOAD_ARG_LONG, WORKLOAD_ARG);
        paramsMap = replaceKey(paramsMap, DB_ARG_LONG, DB_ARG);
        paramsMap = replaceKey(paramsMap, THREADS_ARG_LONG, THREADS_ARG);
        paramsMap = replaceKey(paramsMap, SHOW_STATUS_ARG_LONG, SHOW_STATUS_ARG);
        paramsMap = replaceKey(paramsMap, TIME_UNIT_ARG_LONG, TIME_UNIT_ARG);
        paramsMap = replaceKey(paramsMap, RESULT_FILE_PATH_ARG_LONG, RESULT_FILE_PATH_ARG);
        paramsMap = replaceKey(paramsMap, TIME_COMPRESSION_RATIO_ARG, TIME_COMPRESSION_RATIO_ARG_LONG);
        paramsMap = replaceKey(paramsMap, GCT_DELTA_DURATION_ARG, GCT_DELTA_DURATION_ARG_LONG);
        paramsMap = replaceKey(paramsMap, PEER_IDS_ARG, PEER_IDS_ARG_LONG);
        paramsMap = replaceKey(paramsMap, TOLERATED_EXECUTION_DELAY_ARG, TOLERATED_EXECUTION_DELAY_ARG_LONG);
        return paramsMap;
    }

    // NOTE: not safe in general case, no check for duplicate keys, i.e., if newKey already exists its value will be overwritten
    private static Map<String, String> replaceKey(Map<String, String> paramsMap, String oldKey, String newKey) {
        if (false == paramsMap.containsKey(oldKey)) return paramsMap;
        String value = paramsMap.get(oldKey);
        paramsMap.remove(oldKey);
        paramsMap.put(newKey, value);
        return paramsMap;
    }

    private static Options buildOptions() {
        Options options = new Options();

        /*
         * Required
         */
        Option dbOption = OptionBuilder.hasArgs(1).withArgName("classname").withDescription(DB_DESCRIPTION).withLongOpt(
                DB_ARG_LONG).create(DB_ARG);
        options.addOption(dbOption);

        Option workloadOption = OptionBuilder.hasArgs(1).withArgName("classname").withDescription(
                WORKLOAD_DESCRIPTION).withLongOpt(WORKLOAD_ARG_LONG).create(WORKLOAD_ARG);
        options.addOption(workloadOption);

        Option operationCountOption = OptionBuilder.hasArgs(1).withArgName("count").withDescription(
                OPERATION_COUNT_DESCRIPTION).withLongOpt(OPERATION_COUNT_ARG_LONG).create(OPERATION_COUNT_ARG);
        options.addOption(operationCountOption);

        /*
         * Optional
         */
        Option resultFileOption = OptionBuilder.hasArgs(1).withArgName("path").withDescription(RESULT_FILE_PATH_DESCRIPTION).withLongOpt(
                RESULT_FILE_PATH_ARG_LONG).create(RESULT_FILE_PATH_ARG);
        options.addOption(resultFileOption);

        Option threadsOption = OptionBuilder.hasArgs(1).withArgName("count").withDescription(THREADS_DESCRIPTION).withLongOpt(
                THREADS_ARG_LONG).create(THREADS_ARG);
        options.addOption(threadsOption);

        Option statusOption = OptionBuilder.withDescription(SHOW_STATUS_DESCRIPTION).withLongOpt(
                SHOW_STATUS_ARG_LONG).create(SHOW_STATUS_ARG);
        options.addOption(statusOption);

        Option timeUnitOption = OptionBuilder.hasArgs(1).withArgName("unit").withDescription(TIME_UNIT_DESCRIPTION).withLongOpt(
                TIME_UNIT_ARG_LONG).create(TIME_UNIT_ARG);
        options.addOption(timeUnitOption);

        Option timeCompressionRatioOption = OptionBuilder.hasArgs(1).withArgName("ratio").withDescription(TIME_COMPRESSION_RATIO_DESCRIPTION).withLongOpt(
                TIME_COMPRESSION_RATIO_ARG_LONG).create(TIME_COMPRESSION_RATIO_ARG);
        options.addOption(timeCompressionRatioOption);

        Option gctDeltaDurationOption = OptionBuilder.hasArgs(1).withArgName("duration").withDescription(GCT_DELTA_DURATION_DESCRIPTION).withLongOpt(
                GCT_DELTA_DURATION_ARG_LONG).create(GCT_DELTA_DURATION_ARG);
        options.addOption(gctDeltaDurationOption);

        Option peerIdsOption = OptionBuilder.hasArgs().withValueSeparator(':').withArgName("peerId1:peerId2").withDescription(
                PEER_IDS_DESCRIPTION).withLongOpt(PEER_IDS_ARG_LONG).create(PEER_IDS_ARG);
        options.addOption(peerIdsOption);

        Option toleratedExecutionDelayOption = OptionBuilder.hasArgs(1).withArgName("duration").withDescription(TOLERATED_EXECUTION_DELAY_DESCRIPTION).withLongOpt(
                TOLERATED_EXECUTION_DELAY_ARG_LONG).create(TOLERATED_EXECUTION_DELAY_ARG);
        options.addOption(toleratedExecutionDelayOption);

        Option propertyFileOption = OptionBuilder.hasArgs().withValueSeparator(':').withArgName("file1:file2").withDescription(
                PROPERTY_FILE_DESCRIPTION).create(PROPERTY_FILE_ARG);
        options.addOption(propertyFileOption);

        Option propertyOption = OptionBuilder.hasArgs(2).withValueSeparator('=').withArgName("key=value").withDescription(
                PROPERTY_DESCRIPTION).create(PROPERTY_ARG);
        options.addOption(propertyOption);

        return options;
    }

    static List<String> parsePeerIds(String peerIdsString) throws ParamsException {
        JsonNode jsonArrayNode;
        try {
            jsonArrayNode = new ObjectMapper().readTree(peerIdsString);
        } catch (IOException e) {
            throw new ParamsException(String.format("Peer IDs have been serialized in an invalid format: %s", peerIdsString), e.getCause());
        }
        if (jsonArrayNode.isArray()) {
            List<String> peerIds = new ArrayList<String>();
            for (JsonNode elementNode : jsonArrayNode) {
                peerIds.add(elementNode.asText());
            }
            return peerIds;
        } else {
            throw new ParamsException(String.format("Peer IDs are not a string array, they have been serialized in an invalid format: %s", peerIdsString));
        }
    }

    static String serializePeerIds(List<String> peerIds) throws ParamsException {
        try {
            return new ObjectMapper().writeValueAsString(peerIds);
        } catch (IOException e) {
            throw new ParamsException(String.format("Unable to serialize peer IDs: %s", peerIds.toString()), e.getCause());
        }
    }

    public static String helpString() {
        Options options = OPTIONS;
        int printedRowWidth = 110;
        String header = "";
        String footer = "";
        int spacesBeforeOption = 3;
        int spacesBeforeOptionDescription = 5;
        boolean displayUsage = true;
        String commandLineSyntax = "java -cp core-VERSION.jar " + Client.class.getName();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(os);
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(writer, printedRowWidth, commandLineSyntax, header, options, spacesBeforeOption,
                spacesBeforeOptionDescription, footer, displayUsage);
        writer.flush();
        writer.close();
        return os.toString();
    }

    private final Map<String, String> paramsMap;
    private final String dbClassName;
    private final String workloadClassName;
    private final long operationCount;
    private final int threadCount;
    private final boolean showStatus;
    private final TimeUnit timeUnit;
    private final String resultFilePath;
    private final Double timeCompressionRatio;
    private final Duration gctDeltaDuration;
    private final List<String> peerIds;
    private final Duration toleratedExecutionDelay;

    public WorkloadParams(Map<String, String> paramsMap,
                          String dbClassName,
                          String workloadClassName,
                          long operationCount,
                          int threadCount,
                          boolean showStatus,
                          TimeUnit timeUnit,
                          String resultFilePath,
                          Double timeCompressionRatio,
                          Duration gctDeltaDuration,
                          List<String> peerIds,
                          Duration toleratedExecutionDelay) {
        this.paramsMap = paramsMap;
        this.dbClassName = dbClassName;
        this.workloadClassName = workloadClassName;
        this.operationCount = operationCount;
        this.threadCount = threadCount;
        this.showStatus = showStatus;
        this.timeUnit = timeUnit;
        this.resultFilePath = resultFilePath;
        this.timeCompressionRatio = timeCompressionRatio;
        this.gctDeltaDuration = gctDeltaDuration;
        this.peerIds = peerIds;
        this.toleratedExecutionDelay = toleratedExecutionDelay;
    }

    @Override
    public String dbClassName() {
        return dbClassName;
    }

    @Override
    public String workloadClassName() {
        return workloadClassName;
    }

    @Override
    public long operationCount() {
        return operationCount;
    }

    @Override
    public int threadCount() {
        return threadCount;
    }

    @Override
    public boolean isShowStatus() {
        return showStatus;
    }

    @Override
    public TimeUnit timeUnit() {
        return timeUnit;
    }

    @Override
    public String resultFilePath() {
        return resultFilePath;
    }

    @Override
    public Double timeCompressionRatio() {
        return timeCompressionRatio;
    }

    @Override
    public Duration gctDeltaDuration() {
        return gctDeltaDuration;
    }

    @Override
    public List<String> peerIds() {
        return peerIds;
    }

    @Override
    public Duration toleratedExecutionDelay() {
        return toleratedExecutionDelay;
    }

    @Override
    public Map<String, String> asMap() {
        return paramsMap;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Parameters:").append("\n");
        sb.append("\t").append("DB:\t\t\t").append(dbClassName).append("\n");
        sb.append("\t").append("Workload:\t\t").append(workloadClassName).append("\n");
        sb.append("\t").append("Operation Count:\t").append(operationCount).append("\n");
        sb.append("\t").append("Worker Threads:\t\t").append(threadCount).append("\n");
        sb.append("\t").append("Show Status:\t\t").append(showStatus).append("\n");
        sb.append("\t").append("Time Unit:\t\t").append(timeUnit).append("\n");
        sb.append("\t").append("Result File:\t\t").append(resultFilePath).append("\n");
        sb.append("\t").append("Time Compression Ratio:\t").append(timeCompressionRatio).append("\n");
        sb.append("\t").append("GCT Delta (ms):\t\t").append(gctDeltaDuration.asMilli()).append("\n");
        sb.append("\t").append("Peer IDs:\t\t").append(peerIds.toString()).append("\n");
        sb.append("\t").append("Tolerated Execution Delay (ms):\t").append(toleratedExecutionDelay.asMilli()).append("\n");

        Set<String> excludedKeys = new HashSet<String>();
        excludedKeys.addAll(Arrays.asList(DB_ARG, WORKLOAD_ARG, OPERATION_COUNT_ARG,
                THREADS_ARG, SHOW_STATUS_ARG, TIME_UNIT_ARG, RESULT_FILE_PATH_ARG));
        Map<String, String> filteredParamsMap = MapUtils.copyExcludingKeys(paramsMap, excludedKeys);
        if (false == filteredParamsMap.isEmpty()) {
            sb.append("\t").append("User-defined parameters:").append("\n");
            sb.append(MapUtils.prettyPrint(filteredParamsMap, "\t\t"));
        }

        return sb.toString();
    }
}
