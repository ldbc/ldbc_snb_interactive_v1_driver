package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.DriverConfigurationFileHelper;
import com.ldbc.driver.util.FileUtils;
import com.ldbc.driver.util.MapUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;

public class LdbcSnbInteractiveWorkloadConfiguration
{
    public static final int WRITE_OPERATION_NO_RESULT_DEFAULT_RESULT = -1;
    public final static String LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX = "ldbc.snb.interactive.";
    // directory that contains the substitution parameters files
    public final static String PARAMETERS_DIRECTORY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + "parameters_dir";
    // directory containing forum and person update event streams
    public final static String UPDATES_DIRECTORY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + "updates_dir";

    // Short reads random walk dissipation rate, in the interval [1.0-0.0]
    // Higher values translate to shorter walks and therefore fewer short reads
    public final static String SHORT_READ_DISSIPATION =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + "short_read_dissipation";

    // Average distance between updates in simulation time
    public final static String UPDATE_INTERLEAVE = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + "update_interleave";

    // The parser implementation to use when reading update events
    public enum UpdateStreamParser
    {
        REGEX,
        CHAR_SEEKER,
        CHAR_SEEKER_THREAD
    }

    public final static String UPDATE_STREAM_PARSER = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + "update_parser";
    public final static UpdateStreamParser DEFAULT_UPDATE_STREAM_PARSER = UpdateStreamParser.CHAR_SEEKER;
    public final static String LDBC_INTERACTIVE_PACKAGE_PREFIX =
            removeSuffix( LdbcQuery1.class.getName(), LdbcQuery1.class.getSimpleName() );

    /*
     * Operation Interleave
     */
    public final static String INTERLEAVE_SUFFIX = "_interleave";
    public final static String READ_OPERATION_1_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery1.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_2_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery2.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_3_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery3.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_4_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery4.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_5_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery5.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_6_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery6.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_7_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery7.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_8_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery8.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_9_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery9.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_10_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery10.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_11_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery11.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_12_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery12.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_13_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery13.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String READ_OPERATION_14_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery14.class.getSimpleName() + INTERLEAVE_SUFFIX;
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
            READ_OPERATION_14_INTERLEAVE_KEY );

    /*
     * Operation frequency
     */
    public final static String FREQUENCY_SUFFIX = "_freq";
    public final static String READ_OPERATION_1_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery1.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_2_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery2.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_3_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery3.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_4_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery4.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_5_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery5.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_6_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery6.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_7_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery7.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_8_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery8.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_9_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery9.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_10_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery10.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_11_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery11.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_12_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery12.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_13_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery13.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_14_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery14.class.getSimpleName() + FREQUENCY_SUFFIX;
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

    private static Map<Integer,String> typeToInterleaveKeyMapping()
    {
        Map<Integer,String> mapping = new HashMap<>();
        mapping.put( LdbcQuery1.TYPE, READ_OPERATION_1_INTERLEAVE_KEY );
        mapping.put( LdbcQuery2.TYPE, READ_OPERATION_2_INTERLEAVE_KEY );
        mapping.put( LdbcQuery3.TYPE, READ_OPERATION_3_INTERLEAVE_KEY );
        mapping.put( LdbcQuery4.TYPE, READ_OPERATION_4_INTERLEAVE_KEY );
        mapping.put( LdbcQuery5.TYPE, READ_OPERATION_5_INTERLEAVE_KEY );
        mapping.put( LdbcQuery6.TYPE, READ_OPERATION_6_INTERLEAVE_KEY );
        mapping.put( LdbcQuery7.TYPE, READ_OPERATION_7_INTERLEAVE_KEY );
        mapping.put( LdbcQuery8.TYPE, READ_OPERATION_8_INTERLEAVE_KEY );
        mapping.put( LdbcQuery9.TYPE, READ_OPERATION_9_INTERLEAVE_KEY );
        mapping.put( LdbcQuery10.TYPE, READ_OPERATION_10_INTERLEAVE_KEY );
        mapping.put( LdbcQuery11.TYPE, READ_OPERATION_11_INTERLEAVE_KEY );
        mapping.put( LdbcQuery12.TYPE, READ_OPERATION_12_INTERLEAVE_KEY );
        mapping.put( LdbcQuery13.TYPE, READ_OPERATION_13_INTERLEAVE_KEY );
        mapping.put( LdbcQuery14.TYPE, READ_OPERATION_14_INTERLEAVE_KEY );
        return mapping;
    }

    public final static Map<Integer,String> OPERATION_TYPE_TO_INTERLEAVE_KEY_MAPPING = typeToInterleaveKeyMapping();

    // Default value in case there is no update stream
    public final static String DEFAULT_UPDATE_INTERLEAVE = "1";

    /*
     * Operation Enable
     */
    public final static String ENABLE_SUFFIX = "_enable";
    public final static String LONG_READ_OPERATION_1_ENABLE_KEY = asEnableKey( LdbcQuery1.class );
    public final static String LONG_READ_OPERATION_2_ENABLE_KEY = asEnableKey( LdbcQuery2.class );
    public final static String LONG_READ_OPERATION_3_ENABLE_KEY = asEnableKey( LdbcQuery3.class );
    public final static String LONG_READ_OPERATION_4_ENABLE_KEY = asEnableKey( LdbcQuery4.class );
    public final static String LONG_READ_OPERATION_5_ENABLE_KEY = asEnableKey( LdbcQuery5.class );
    public final static String LONG_READ_OPERATION_6_ENABLE_KEY = asEnableKey( LdbcQuery6.class );
    public final static String LONG_READ_OPERATION_7_ENABLE_KEY = asEnableKey( LdbcQuery7.class );
    public final static String LONG_READ_OPERATION_8_ENABLE_KEY = asEnableKey( LdbcQuery8.class );
    public final static String LONG_READ_OPERATION_9_ENABLE_KEY = asEnableKey( LdbcQuery9.class );
    public final static String LONG_READ_OPERATION_10_ENABLE_KEY = asEnableKey( LdbcQuery10.class );
    public final static String LONG_READ_OPERATION_11_ENABLE_KEY = asEnableKey( LdbcQuery11.class );
    public final static String LONG_READ_OPERATION_12_ENABLE_KEY = asEnableKey( LdbcQuery12.class );
    public final static String LONG_READ_OPERATION_13_ENABLE_KEY = asEnableKey( LdbcQuery13.class );
    public final static String LONG_READ_OPERATION_14_ENABLE_KEY = asEnableKey( LdbcQuery14.class );
    public final static List<String> LONG_READ_OPERATION_ENABLE_KEYS = Lists.newArrayList(
            LONG_READ_OPERATION_1_ENABLE_KEY,
            LONG_READ_OPERATION_2_ENABLE_KEY,
            LONG_READ_OPERATION_3_ENABLE_KEY,
            LONG_READ_OPERATION_4_ENABLE_KEY,
            LONG_READ_OPERATION_5_ENABLE_KEY,
            LONG_READ_OPERATION_6_ENABLE_KEY,
            LONG_READ_OPERATION_7_ENABLE_KEY,
            LONG_READ_OPERATION_8_ENABLE_KEY,
            LONG_READ_OPERATION_9_ENABLE_KEY,
            LONG_READ_OPERATION_10_ENABLE_KEY,
            LONG_READ_OPERATION_11_ENABLE_KEY,
            LONG_READ_OPERATION_12_ENABLE_KEY,
            LONG_READ_OPERATION_13_ENABLE_KEY,
            LONG_READ_OPERATION_14_ENABLE_KEY
    );
    public final static String SHORT_READ_OPERATION_1_ENABLE_KEY = asEnableKey( LdbcShortQuery1PersonProfile.class );
    public final static String SHORT_READ_OPERATION_2_ENABLE_KEY = asEnableKey( LdbcShortQuery2PersonPosts.class );
    public final static String SHORT_READ_OPERATION_3_ENABLE_KEY = asEnableKey( LdbcShortQuery3PersonFriends.class );
    public final static String SHORT_READ_OPERATION_4_ENABLE_KEY = asEnableKey( LdbcShortQuery4MessageContent.class );
    public final static String SHORT_READ_OPERATION_5_ENABLE_KEY = asEnableKey( LdbcShortQuery5MessageCreator.class );
    public final static String SHORT_READ_OPERATION_6_ENABLE_KEY = asEnableKey( LdbcShortQuery6MessageForum.class );
    public final static String SHORT_READ_OPERATION_7_ENABLE_KEY = asEnableKey( LdbcShortQuery7MessageReplies.class );
    public final static List<String> SHORT_READ_OPERATION_ENABLE_KEYS = Lists.newArrayList(
            SHORT_READ_OPERATION_1_ENABLE_KEY,
            SHORT_READ_OPERATION_2_ENABLE_KEY,
            SHORT_READ_OPERATION_3_ENABLE_KEY,
            SHORT_READ_OPERATION_4_ENABLE_KEY,
            SHORT_READ_OPERATION_5_ENABLE_KEY,
            SHORT_READ_OPERATION_6_ENABLE_KEY,
            SHORT_READ_OPERATION_7_ENABLE_KEY
    );
    public final static String WRITE_OPERATION_1_ENABLE_KEY = asEnableKey( LdbcUpdate1AddPerson.class );
    public final static String WRITE_OPERATION_2_ENABLE_KEY = asEnableKey( LdbcUpdate2AddPostLike.class );
    public final static String WRITE_OPERATION_3_ENABLE_KEY = asEnableKey( LdbcUpdate3AddCommentLike.class );
    public final static String WRITE_OPERATION_4_ENABLE_KEY = asEnableKey( LdbcUpdate4AddForum.class );
    public final static String WRITE_OPERATION_5_ENABLE_KEY = asEnableKey( LdbcUpdate5AddForumMembership.class );
    public final static String WRITE_OPERATION_6_ENABLE_KEY = asEnableKey( LdbcUpdate6AddPost.class );
    public final static String WRITE_OPERATION_7_ENABLE_KEY = asEnableKey( LdbcUpdate7AddComment.class );
    public final static String WRITE_OPERATION_8_ENABLE_KEY = asEnableKey( LdbcUpdate8AddFriendship.class );
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

    private static String asEnableKey( Class<? extends Operation> operation )
    {
        return LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + operation.getSimpleName() + ENABLE_SUFFIX;
    }

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

    public static Map<String,String> convertFrequenciesToInterleaves( Map<String,String> params )
    {
        Integer updateDistance = Integer.parseInt( params.get( UPDATE_INTERLEAVE ) );

        Integer interleave = Integer.parseInt( params.get( READ_OPERATION_1_FREQUENCY_KEY ) ) * updateDistance;
        params.put( READ_OPERATION_1_INTERLEAVE_KEY, interleave.toString() );

        interleave = Integer.parseInt( params.get( READ_OPERATION_2_FREQUENCY_KEY ) ) * updateDistance;
        params.put( READ_OPERATION_2_INTERLEAVE_KEY, interleave.toString() );

        interleave = Integer.parseInt( params.get( READ_OPERATION_3_FREQUENCY_KEY ) ) * updateDistance;
        params.put( READ_OPERATION_3_INTERLEAVE_KEY, interleave.toString() );

        interleave = Integer.parseInt( params.get( READ_OPERATION_4_FREQUENCY_KEY ) ) * updateDistance;
        params.put( READ_OPERATION_4_INTERLEAVE_KEY, interleave.toString() );

        interleave = Integer.parseInt( params.get( READ_OPERATION_5_FREQUENCY_KEY ) ) * updateDistance;
        params.put( READ_OPERATION_5_INTERLEAVE_KEY, interleave.toString() );

        interleave = Integer.parseInt( params.get( READ_OPERATION_6_FREQUENCY_KEY ) ) * updateDistance;
        params.put( READ_OPERATION_6_INTERLEAVE_KEY, interleave.toString() );

        interleave = Integer.parseInt( params.get( READ_OPERATION_7_FREQUENCY_KEY ) ) * updateDistance;
        params.put( READ_OPERATION_7_INTERLEAVE_KEY, interleave.toString() );

        interleave = Integer.parseInt( params.get( READ_OPERATION_8_FREQUENCY_KEY ) ) * updateDistance;
        params.put( READ_OPERATION_8_INTERLEAVE_KEY, interleave.toString() );

        interleave = Integer.parseInt( params.get( READ_OPERATION_9_FREQUENCY_KEY ) ) * updateDistance;
        params.put( READ_OPERATION_9_INTERLEAVE_KEY, interleave.toString() );

        interleave = Integer.parseInt( params.get( READ_OPERATION_10_FREQUENCY_KEY ) ) * updateDistance;
        params.put( READ_OPERATION_10_INTERLEAVE_KEY, interleave.toString() );

        interleave = Integer.parseInt( params.get( READ_OPERATION_11_FREQUENCY_KEY ) ) * updateDistance;
        params.put( READ_OPERATION_11_INTERLEAVE_KEY, interleave.toString() );

        interleave = Integer.parseInt( params.get( READ_OPERATION_12_FREQUENCY_KEY ) ) * updateDistance;
        params.put( READ_OPERATION_12_INTERLEAVE_KEY, interleave.toString() );

        interleave = Integer.parseInt( params.get( READ_OPERATION_13_FREQUENCY_KEY ) ) * updateDistance;
        params.put( READ_OPERATION_13_INTERLEAVE_KEY, interleave.toString() );

        interleave = Integer.parseInt( params.get( READ_OPERATION_14_FREQUENCY_KEY ) ) * updateDistance;
        params.put( READ_OPERATION_14_INTERLEAVE_KEY, interleave.toString() );

        return params;
    }

    public static File defaultConfigFileSF1() throws DriverConfigurationException
    {
        return defaultConfigFileSF1FromWorkloadsDirectory(
                DriverConfigurationFileHelper.getWorkloadsDirectory()
        );
    }

    public static File defaultConfigFileSF1( File driverRootDirectory ) throws DriverConfigurationException
    {
        return defaultConfigFileSF1FromWorkloadsDirectory(
                DriverConfigurationFileHelper.getWorkloadsDirectory( driverRootDirectory )
        );
    }

    private static File defaultConfigFileSF1FromWorkloadsDirectory( File workloadsDirectory )
            throws DriverConfigurationException
    {
        return new File( workloadsDirectory, "ldbc/snb/interactive/ldbc_snb_interactive_SF-0001.properties" );
    }

    public static Map<String,String> defaultConfigSF1() throws DriverConfigurationException, IOException
    {
        return ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys(
                MapUtils.loadPropertiesToMap(
                        defaultConfigFileSF1()
                )
        );
    }

    public static Map<String,String> defaultReadOnlyConfigSF1() throws DriverConfigurationException, IOException
    {
        Map<String,String> params = withoutWrites(
                defaultConfigSF1()
        );
        return ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys( params );
    }

    public static Map<String,String> withoutOnly(
            Map<String,String> originalParams,
            Class<? extends Operation>... operationClasses )
            throws DriverConfigurationException, IOException
    {
        Map<String,String> params = withoutWrites(
                withoutShortReads(
                        withoutLongReads( originalParams )
                )
        );
        for ( Class<? extends Operation> operationClass : operationClasses )
        {
            params.put( asEnableKey( operationClass ), "true" );
        }
        return ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys( params );
    }

    public static Map<String,String> withoutShortReads( Map<String,String> originalParams )
            throws DriverConfigurationException, IOException
    {
        Map<String,String> params = MapUtils.copyExcludingKeys( originalParams, new HashSet<String>() );
        params.put( SHORT_READ_OPERATION_1_ENABLE_KEY, "false" );
        params.put( SHORT_READ_OPERATION_2_ENABLE_KEY, "false" );
        params.put( SHORT_READ_OPERATION_3_ENABLE_KEY, "false" );
        params.put( SHORT_READ_OPERATION_4_ENABLE_KEY, "false" );
        params.put( SHORT_READ_OPERATION_5_ENABLE_KEY, "false" );
        params.put( SHORT_READ_OPERATION_6_ENABLE_KEY, "false" );
        params.put( SHORT_READ_OPERATION_7_ENABLE_KEY, "false" );
        return ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys( params );
    }

    public static Map<String,String> withoutWrites( Map<String,String> originalParams )
            throws DriverConfigurationException, IOException
    {
        Map<String,String> params = MapUtils.copyExcludingKeys( originalParams, new HashSet<String>() );
        params.put( WRITE_OPERATION_1_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_2_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_3_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_4_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_5_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_6_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_7_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_8_ENABLE_KEY, "false" );
        return ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys( params );
    }

    public static Map<String,String> withoutLongReads( Map<String,String> originalParams )
            throws DriverConfigurationException, IOException
    {
        Map<String,String> params = MapUtils.copyExcludingKeys( originalParams, new HashSet<String>() );
        params.put( LONG_READ_OPERATION_1_ENABLE_KEY, "false" );
        params.put( LONG_READ_OPERATION_2_ENABLE_KEY, "false" );
        params.put( LONG_READ_OPERATION_3_ENABLE_KEY, "false" );
        params.put( LONG_READ_OPERATION_4_ENABLE_KEY, "false" );
        params.put( LONG_READ_OPERATION_5_ENABLE_KEY, "false" );
        params.put( LONG_READ_OPERATION_6_ENABLE_KEY, "false" );
        params.put( LONG_READ_OPERATION_7_ENABLE_KEY, "false" );
        params.put( LONG_READ_OPERATION_8_ENABLE_KEY, "false" );
        params.put( LONG_READ_OPERATION_9_ENABLE_KEY, "false" );
        params.put( LONG_READ_OPERATION_10_ENABLE_KEY, "false" );
        params.put( LONG_READ_OPERATION_11_ENABLE_KEY, "false" );
        params.put( LONG_READ_OPERATION_12_ENABLE_KEY, "false" );
        params.put( LONG_READ_OPERATION_13_ENABLE_KEY, "false" );
        params.put( LONG_READ_OPERATION_14_ENABLE_KEY, "false" );
        params.put( SHORT_READ_OPERATION_1_ENABLE_KEY, "false" );
        params.put( SHORT_READ_OPERATION_2_ENABLE_KEY, "false" );
        params.put( SHORT_READ_OPERATION_3_ENABLE_KEY, "false" );
        params.put( SHORT_READ_OPERATION_4_ENABLE_KEY, "false" );
        params.put( SHORT_READ_OPERATION_5_ENABLE_KEY, "false" );
        params.put( SHORT_READ_OPERATION_6_ENABLE_KEY, "false" );
        params.put( SHORT_READ_OPERATION_7_ENABLE_KEY, "false" );
        return ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys( params );
    }

    public static Map<Integer,Class<? extends Operation>> operationTypeToClassMapping()
    {
        Map<Integer,Class<? extends Operation>> operationTypeToClassMapping = new HashMap<>();
        operationTypeToClassMapping.put( LdbcQuery1.TYPE, LdbcQuery1.class );
        operationTypeToClassMapping.put( LdbcQuery2.TYPE, LdbcQuery2.class );
        operationTypeToClassMapping.put( LdbcQuery3.TYPE, LdbcQuery3.class );
        operationTypeToClassMapping.put( LdbcQuery4.TYPE, LdbcQuery4.class );
        operationTypeToClassMapping.put( LdbcQuery5.TYPE, LdbcQuery5.class );
        operationTypeToClassMapping.put( LdbcQuery6.TYPE, LdbcQuery6.class );
        operationTypeToClassMapping.put( LdbcQuery7.TYPE, LdbcQuery7.class );
        operationTypeToClassMapping.put( LdbcQuery8.TYPE, LdbcQuery8.class );
        operationTypeToClassMapping.put( LdbcQuery9.TYPE, LdbcQuery9.class );
        operationTypeToClassMapping.put( LdbcQuery10.TYPE, LdbcQuery10.class );
        operationTypeToClassMapping.put( LdbcQuery11.TYPE, LdbcQuery11.class );
        operationTypeToClassMapping.put( LdbcQuery12.TYPE, LdbcQuery12.class );
        operationTypeToClassMapping.put( LdbcQuery13.TYPE, LdbcQuery13.class );
        operationTypeToClassMapping.put( LdbcQuery14.TYPE, LdbcQuery14.class );
        operationTypeToClassMapping.put( LdbcShortQuery1PersonProfile.TYPE, LdbcShortQuery1PersonProfile.class );
        operationTypeToClassMapping.put( LdbcShortQuery2PersonPosts.TYPE, LdbcShortQuery2PersonPosts.class );
        operationTypeToClassMapping.put( LdbcShortQuery3PersonFriends.TYPE, LdbcShortQuery3PersonFriends.class );
        operationTypeToClassMapping.put( LdbcShortQuery4MessageContent.TYPE, LdbcShortQuery4MessageContent.class );
        operationTypeToClassMapping.put( LdbcShortQuery5MessageCreator.TYPE, LdbcShortQuery5MessageCreator.class );
        operationTypeToClassMapping.put( LdbcShortQuery6MessageForum.TYPE, LdbcShortQuery6MessageForum.class );
        operationTypeToClassMapping.put( LdbcShortQuery7MessageReplies.TYPE, LdbcShortQuery7MessageReplies.class );
        operationTypeToClassMapping.put( LdbcUpdate1AddPerson.TYPE, LdbcUpdate1AddPerson.class );
        operationTypeToClassMapping.put( LdbcUpdate2AddPostLike.TYPE, LdbcUpdate2AddPostLike.class );
        operationTypeToClassMapping.put( LdbcUpdate3AddCommentLike.TYPE, LdbcUpdate3AddCommentLike.class );
        operationTypeToClassMapping.put( LdbcUpdate4AddForum.TYPE, LdbcUpdate4AddForum.class );
        operationTypeToClassMapping.put( LdbcUpdate5AddForumMembership.TYPE, LdbcUpdate5AddForumMembership.class );
        operationTypeToClassMapping.put( LdbcUpdate6AddPost.TYPE, LdbcUpdate6AddPost.class );
        operationTypeToClassMapping.put( LdbcUpdate7AddComment.TYPE, LdbcUpdate7AddComment.class );
        operationTypeToClassMapping.put( LdbcUpdate8AddFriendship.TYPE, LdbcUpdate8AddFriendship.class );
        return operationTypeToClassMapping;
    }

    static String removeSuffix( String original, String suffix )
    {
        return (!original.contains( suffix )) ? original : original.substring( 0, original.lastIndexOf( suffix ) );
    }

    static String removePrefix( String original, String prefix )
    {
        return (!original.contains( prefix )) ? original : original
                .substring( original.lastIndexOf( prefix ) + prefix.length(), original.length() );
    }

    static Set<String> missingParameters( Map<String,String> properties, Iterable<String> compulsoryPropertyKeys )
    {
        Set<String> missingPropertyKeys = new HashSet<>();
        for ( String compulsoryKey : compulsoryPropertyKeys )
        {
            if ( null == properties.get( compulsoryKey ) )
            { missingPropertyKeys.add( compulsoryKey ); }
        }
        return missingPropertyKeys;
    }

    static boolean isValidParser( String parserString ) throws WorkloadException
    {
        try
        {
            UpdateStreamParser parser = UpdateStreamParser.valueOf( parserString );
            Set<UpdateStreamParser> validParsers = new HashSet<>();
            validParsers.addAll( Arrays.asList( UpdateStreamParser.values() ) );
            return validParsers.contains( parser );
        }
        catch ( IllegalArgumentException e )
        {
            throw new WorkloadException( format( "Unsupported parser value: %s", parserString ), e );
        }
    }

    public static List<File> forumUpdateFilesInDirectory( File directory )
    {
        return FileUtils.filesWithSuffixInDirectory( directory, "_forum.csv" );
    }

    public static List<File> personUpdateFilesInDirectory( File directory )
    {
        return FileUtils.filesWithSuffixInDirectory( directory, "_person.csv" );
    }
}
