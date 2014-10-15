package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.Lists;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;

import java.util.*;

public class LdbcSnbInteractiveConfiguration {
    public final static String LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX = "ldbc.snb.interactive.";
    // directory that contains the substitution parameters files
    public final static String PARAMETERS_DIRECTORY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + "parameters_dir";
    // list of paths to forum update event streams
    public final static String FORUM_UPDATE_FILES = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + "forum_update_files";
    // list of paths to person update event streams
    public final static String PERSON_UPDATE_FILES = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + "person_update_files";
    // minimum duration between any two dependent operations in the update streams
    public final static String SAFE_T = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + "gct_delta_duration";
    // Average distance between updates in simulation time
    public final static String UPDATE_INTERLEAVE = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + "update_interleave";
    public final static String LDBC_INTERACTIVE_PACKAGE_PREFIX = removeSuffix(LdbcQuery1.class.getName(), LdbcQuery1.class.getSimpleName());

    /*
     * Operation Interleave
     */
    public final static String INTERLEAVE_SUFFIX = "_interleave";
    public final static String READ_OPERATION_1_INTERLEAVE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery1.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_2_INTERLEAVE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery2.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_3_INTERLEAVE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery3.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_4_INTERLEAVE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery4.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_5_INTERLEAVE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery5.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_6_INTERLEAVE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery6.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_7_INTERLEAVE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery7.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_8_INTERLEAVE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery8.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_9_INTERLEAVE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery9.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_10_INTERLEAVE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery10.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_11_INTERLEAVE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery11.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_12_INTERLEAVE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery12.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_13_INTERLEAVE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery13.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_14_INTERLEAVE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery14.class.getSimpleName() + INTERLEAVE_SUFFIX;
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
     * Operation frequency
     */
    public final static String FREQUENCY_SUFFIX = "_freq";
    public final static String READ_OPERATION_1_FREQUENCY_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery1.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_2_FREQUENCY_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery2.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_3_FREQUENCY_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery3.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_4_FREQUENCY_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery4.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_5_FREQUENCY_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery5.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_6_FREQUENCY_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery6.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_7_FREQUENCY_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery7.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_8_FREQUENCY_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery8.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_9_FREQUENCY_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery9.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_10_FREQUENCY_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery10.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_11_FREQUENCY_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery11.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_12_FREQUENCY_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery12.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_13_FREQUENCY_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery13.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_14_FREQUENCY_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery14.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static List<String> READ_OPERATION_FREQUENCY_KEYS = Lists.newArrayList(
            READ_OPERATION_1_FREQUENCY_KEY,
            READ_OPERATION_2_FREQUENCY_KEY,
            READ_OPERATION_3_FREQUENCY_KEY,
            READ_OPERATION_4_FREQUENCY_KEY,
            READ_OPERATION_5_FREQUENCY_KEY,
            READ_OPERATION_6_FREQUENCY_KEY,
            READ_OPERATION_7_FREQUENCY_KEY,
            READ_OPERATION_8_FREQUENCY_KEY,
            READ_OPERATION_9_FREQUENCY_KEY,
            READ_OPERATION_10_FREQUENCY_KEY,
            READ_OPERATION_11_FREQUENCY_KEY,
            READ_OPERATION_12_FREQUENCY_KEY,
            READ_OPERATION_13_FREQUENCY_KEY,
            READ_OPERATION_14_FREQUENCY_KEY
    );


    // Default value in case there is no update stream
    public final static String DEFAULT_UPDATE_INTERLEAVE = "1";

    /*
     * Operation Enable
     */
    public final static String ENABLE_SUFFIX = "_enable";
    public final static String READ_OPERATION_1_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery1.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_2_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery2.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_3_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery3.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_4_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery4.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_5_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery5.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_6_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery6.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_7_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery7.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_8_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery8.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_9_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery9.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_10_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery10.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_11_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery11.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_12_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery12.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_13_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery13.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String READ_OPERATION_14_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery14.class.getSimpleName() + ENABLE_SUFFIX;
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
    public final static String WRITE_OPERATION_1_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcUpdate1AddPerson.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String WRITE_OPERATION_2_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcUpdate2AddPostLike.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String WRITE_OPERATION_3_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcUpdate3AddCommentLike.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String WRITE_OPERATION_4_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcUpdate4AddForum.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String WRITE_OPERATION_5_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcUpdate5AddForumMembership.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String WRITE_OPERATION_6_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcUpdate6AddPost.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String WRITE_OPERATION_7_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcUpdate7AddComment.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String WRITE_OPERATION_8_ENABLE_KEY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcUpdate8AddFriendship.class.getSimpleName() + ENABLE_SUFFIX;
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
    public final static String PIPE_SEPARATOR_REGEX = "\\|";
    public final static String PIPE_SEPARATOR = "|";

    public static Map<String, String> convertFrequenciesToInterleaves(Map<String, String> params) {
        Integer updateDistance = Integer.parseInt(params.get(UPDATE_INTERLEAVE));

        Integer interleave = Integer.parseInt(params.get(READ_OPERATION_1_FREQUENCY_KEY)) * updateDistance;
        params.put(READ_OPERATION_1_INTERLEAVE_KEY, interleave.toString());

        interleave = Integer.parseInt(params.get(READ_OPERATION_2_FREQUENCY_KEY)) * updateDistance;
        params.put(READ_OPERATION_2_INTERLEAVE_KEY, interleave.toString());

        interleave = Integer.parseInt(params.get(READ_OPERATION_3_FREQUENCY_KEY)) * updateDistance;
        params.put(READ_OPERATION_3_INTERLEAVE_KEY, interleave.toString());

        interleave = Integer.parseInt(params.get(READ_OPERATION_4_FREQUENCY_KEY)) * updateDistance;
        params.put(READ_OPERATION_4_INTERLEAVE_KEY, interleave.toString());

        interleave = Integer.parseInt(params.get(READ_OPERATION_5_FREQUENCY_KEY)) * updateDistance;
        params.put(READ_OPERATION_5_INTERLEAVE_KEY, interleave.toString());

        interleave = Integer.parseInt(params.get(READ_OPERATION_6_FREQUENCY_KEY)) * updateDistance;
        params.put(READ_OPERATION_6_INTERLEAVE_KEY, interleave.toString());

        interleave = Integer.parseInt(params.get(READ_OPERATION_7_FREQUENCY_KEY)) * updateDistance;
        params.put(READ_OPERATION_7_INTERLEAVE_KEY, interleave.toString());

        interleave = Integer.parseInt(params.get(READ_OPERATION_8_FREQUENCY_KEY)) * updateDistance;
        params.put(READ_OPERATION_8_INTERLEAVE_KEY, interleave.toString());

        interleave = Integer.parseInt(params.get(READ_OPERATION_9_FREQUENCY_KEY)) * updateDistance;
        params.put(READ_OPERATION_9_INTERLEAVE_KEY, interleave.toString());

        interleave = Integer.parseInt(params.get(READ_OPERATION_10_FREQUENCY_KEY)) * updateDistance;
        params.put(READ_OPERATION_10_INTERLEAVE_KEY, interleave.toString());

        interleave = Integer.parseInt(params.get(READ_OPERATION_11_FREQUENCY_KEY)) * updateDistance;
        params.put(READ_OPERATION_11_INTERLEAVE_KEY, interleave.toString());

        interleave = Integer.parseInt(params.get(READ_OPERATION_12_FREQUENCY_KEY)) * updateDistance;
        params.put(READ_OPERATION_12_INTERLEAVE_KEY, interleave.toString());

        interleave = Integer.parseInt(params.get(READ_OPERATION_13_FREQUENCY_KEY)) * updateDistance;
        params.put(READ_OPERATION_13_INTERLEAVE_KEY, interleave.toString());

        interleave = Integer.parseInt(params.get(READ_OPERATION_14_FREQUENCY_KEY)) * updateDistance;
        params.put(READ_OPERATION_14_INTERLEAVE_KEY, interleave.toString());

        return params;
    }

    public static Map<String, String> defaultConfig() {
        Map<String, String> params = new HashMap<>();
        // General Driver parameters
        params.put(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "1000");
        params.put(ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, LdbcSnbInteractiveWorkload.class.getName());
        // LDBC Interactive Workload-specific parameters
        // reads: set frequency
        params.put(READ_OPERATION_1_FREQUENCY_KEY, "205");
        params.put(READ_OPERATION_2_FREQUENCY_KEY, "406");
        params.put(READ_OPERATION_3_FREQUENCY_KEY, "42");
        params.put(READ_OPERATION_4_FREQUENCY_KEY, "6271");
        params.put(READ_OPERATION_5_FREQUENCY_KEY, "179");
        params.put(READ_OPERATION_6_FREQUENCY_KEY, "18");
        params.put(READ_OPERATION_7_FREQUENCY_KEY, "9235");
        params.put(READ_OPERATION_8_FREQUENCY_KEY, "30445");
        params.put(READ_OPERATION_9_FREQUENCY_KEY, "19");
        params.put(READ_OPERATION_10_FREQUENCY_KEY, "45");
        params.put(READ_OPERATION_11_FREQUENCY_KEY, "3069");
        params.put(READ_OPERATION_12_FREQUENCY_KEY, "155");
        params.put(READ_OPERATION_13_FREQUENCY_KEY, "204");
        params.put(READ_OPERATION_14_FREQUENCY_KEY, "109");
        params.put(READ_OPERATION_1_ENABLE_KEY, "true");
        params.put(READ_OPERATION_2_ENABLE_KEY, "true");
        params.put(READ_OPERATION_3_ENABLE_KEY, "true");
        params.put(READ_OPERATION_4_ENABLE_KEY, "true");
        params.put(READ_OPERATION_5_ENABLE_KEY, "true");
        params.put(READ_OPERATION_6_ENABLE_KEY, "true");
        params.put(READ_OPERATION_7_ENABLE_KEY, "true");
        params.put(READ_OPERATION_8_ENABLE_KEY, "true");
        params.put(READ_OPERATION_9_ENABLE_KEY, "true");
        params.put(READ_OPERATION_10_ENABLE_KEY, "true");
        params.put(READ_OPERATION_11_ENABLE_KEY, "true");
        params.put(READ_OPERATION_12_ENABLE_KEY, "true");
        params.put(READ_OPERATION_13_ENABLE_KEY, "true");
        params.put(READ_OPERATION_14_ENABLE_KEY, "true");
        // writes
        params.put(WRITE_OPERATION_1_ENABLE_KEY, "true");
        params.put(WRITE_OPERATION_2_ENABLE_KEY, "true");
        params.put(WRITE_OPERATION_3_ENABLE_KEY, "true");
        params.put(WRITE_OPERATION_4_ENABLE_KEY, "true");
        params.put(WRITE_OPERATION_5_ENABLE_KEY, "true");
        params.put(WRITE_OPERATION_6_ENABLE_KEY, "true");
        params.put(WRITE_OPERATION_7_ENABLE_KEY, "true");
        params.put(WRITE_OPERATION_8_ENABLE_KEY, "true");
        return ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys(params);
    }

    public static Map<String, String> defaultReadOnlyConfig() {
        Map<String, String> params = defaultConfig();
        params.put(WRITE_OPERATION_1_ENABLE_KEY, "false");
        params.put(WRITE_OPERATION_2_ENABLE_KEY, "false");
        params.put(WRITE_OPERATION_3_ENABLE_KEY, "false");
        params.put(WRITE_OPERATION_4_ENABLE_KEY, "false");
        params.put(WRITE_OPERATION_5_ENABLE_KEY, "false");
        params.put(WRITE_OPERATION_6_ENABLE_KEY, "false");
        params.put(WRITE_OPERATION_7_ENABLE_KEY, "false");
        params.put(WRITE_OPERATION_8_ENABLE_KEY, "false");
        return ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys(params);
    }

    public static Map<String, String> defaultWriteOnlyConfig() {
        Map<String, String> params = defaultConfig();
        params.put(READ_OPERATION_1_ENABLE_KEY, "false");
        params.put(READ_OPERATION_2_ENABLE_KEY, "false");
        params.put(READ_OPERATION_3_ENABLE_KEY, "false");
        params.put(READ_OPERATION_4_ENABLE_KEY, "false");
        params.put(READ_OPERATION_5_ENABLE_KEY, "false");
        params.put(READ_OPERATION_6_ENABLE_KEY, "false");
        params.put(READ_OPERATION_7_ENABLE_KEY, "false");
        params.put(READ_OPERATION_8_ENABLE_KEY, "false");
        params.put(READ_OPERATION_9_ENABLE_KEY, "false");
        params.put(READ_OPERATION_10_ENABLE_KEY, "false");
        params.put(READ_OPERATION_11_ENABLE_KEY, "false");
        params.put(READ_OPERATION_12_ENABLE_KEY, "false");
        params.put(READ_OPERATION_13_ENABLE_KEY, "false");
        params.put(READ_OPERATION_14_ENABLE_KEY, "false");
        return ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys(params);
    }

    static String removeSuffix(String original, String suffix) {
        return (original.indexOf(suffix) == -1) ? original : original.substring(0, original.lastIndexOf(suffix));
    }

    static String removePrefix(String original, String prefix) {
        return (original.indexOf(prefix) == -1) ? original : original.substring(original.lastIndexOf(prefix) + prefix.length(), original.length());
    }

    static Set<String> missingParameters(Map<String, String> properties, Iterable<String> compulsoryPropertyKeys) {
        Set<String> missingPropertyKeys = new HashSet<>();
        for (String compulsoryKey : compulsoryPropertyKeys) {
            if (null == properties.get(compulsoryKey)) missingPropertyKeys.add(compulsoryKey);
        }
        return missingPropertyKeys;
    }

    public static Set<String> parseFilePathsListFromConfiguration(String filePaths) {
        Set<String> filePathsSet = new HashSet<>();
        String[] filePathsArray = filePaths.split(PIPE_SEPARATOR_REGEX);
        for (String filePath : filePathsArray) {
            if (filePath.isEmpty()) continue;
            filePathsSet.add(filePath);
        }
        return filePathsSet;
    }

    public static String serializeFilePathsListFromConfiguration(Iterable<String> filePaths) {
        List<String> filePathsList = Lists.newArrayList(filePaths);

        if (0 == filePathsList.size())
            return "";

        if (1 == filePathsList.size())
            return filePathsList.get(0);

        String filePathsString = "";
        for (int i = 0; i < filePathsList.size() - 1; i++) {
            filePathsString += filePathsList.get(i) + PIPE_SEPARATOR;
        }
        filePathsString += filePathsList.get(filePathsList.size() - 1);

        return filePathsString;
    }
}
