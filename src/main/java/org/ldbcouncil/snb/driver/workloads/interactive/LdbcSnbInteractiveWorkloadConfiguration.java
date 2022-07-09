package org.ldbcouncil.snb.driver.workloads.interactive;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.control.ConsoleAndFileDriverConfiguration;
import org.ldbcouncil.snb.driver.control.DriverConfigurationException;
import org.ldbcouncil.snb.driver.util.MapUtils;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static java.lang.String.format;

public abstract class LdbcSnbInteractiveWorkloadConfiguration
{
    public static final int WRITE_OPERATION_NO_RESULT_DEFAULT_RESULT = -1;
    public static final String LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX = "ldbc.snb.interactive.";
    // directory that contains the substitution parameters files
    public static final String PARAMETERS_DIRECTORY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + "parameters_dir";
    // directory containing forum and person update event streams
    public static final String UPDATES_DIRECTORY = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + "updates_dir";

    // Short reads random walk dissipation rate, in the interval [1.0-0.0]
    // Higher values translate to shorter walks and therefore fewer short reads
    public static final String SHORT_READ_DISSIPATION =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + "short_read_dissipation";

    // Average distance between updates in simulation time
    public static final String UPDATE_INTERLEAVE = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + "update_interleave";

    public static final String SCALE_FACTOR = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + "scale_factor";

    // The parser implementation to use when reading update events
    public enum UpdateStreamParser
    {
        REGEX,
        CHAR_SEEKER,
        CHAR_SEEKER_THREAD
    }

    public static final String INSERTS_DIRECTORY = "inserts";
    public static final String DELETES_DIRECTORY = "deletes";

    public static final String UPDATE_STREAM_PARSER = LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + "update_parser";
    public static final UpdateStreamParser DEFAULT_UPDATE_STREAM_PARSER = UpdateStreamParser.CHAR_SEEKER;
    public static final String LDBC_INTERACTIVE_PACKAGE_PREFIX =
            removeSuffix( LdbcQuery1.class.getName(), LdbcQuery1.class.getSimpleName() );

    /*
     * Operation Interleave
     */
    public static final String INTERLEAVE_SUFFIX = "_interleave";
    public static final String READ_OPERATION_1_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery1.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public static final String READ_OPERATION_2_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery2.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public static final String READ_OPERATION_3_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery3.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public static final String READ_OPERATION_4_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery4.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public static final String READ_OPERATION_5_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery5.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public static final String READ_OPERATION_6_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery6.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public static final String READ_OPERATION_7_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery7.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public static final String READ_OPERATION_8_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery8.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public static final String READ_OPERATION_9_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery9.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public static final String READ_OPERATION_10_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery10.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public static final String READ_OPERATION_11_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery11.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public static final String READ_OPERATION_12_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery12.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public static final String READ_OPERATION_13_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery13.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public static final String READ_OPERATION_14_INTERLEAVE_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery14.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public static final List<String> READ_OPERATION_INTERLEAVE_KEYS = Lists.newArrayList(
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
    public static final String FREQUENCY_SUFFIX = "_freq";
    public static final String READ_OPERATION_1_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery1.class.getSimpleName() + FREQUENCY_SUFFIX;
    public static final String READ_OPERATION_2_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery2.class.getSimpleName() + FREQUENCY_SUFFIX;
    public static final String READ_OPERATION_3_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery3.class.getSimpleName() + FREQUENCY_SUFFIX;
    public static final String READ_OPERATION_4_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery4.class.getSimpleName() + FREQUENCY_SUFFIX;
    public static final String READ_OPERATION_5_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery5.class.getSimpleName() + FREQUENCY_SUFFIX;
    public static final String READ_OPERATION_6_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery6.class.getSimpleName() + FREQUENCY_SUFFIX;
    public static final String READ_OPERATION_7_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery7.class.getSimpleName() + FREQUENCY_SUFFIX;
    public static final String READ_OPERATION_8_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery8.class.getSimpleName() + FREQUENCY_SUFFIX;
    public static final String READ_OPERATION_9_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery9.class.getSimpleName() + FREQUENCY_SUFFIX;
    public static final String READ_OPERATION_10_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery10.class.getSimpleName() + FREQUENCY_SUFFIX;
    public static final String READ_OPERATION_11_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery11.class.getSimpleName() + FREQUENCY_SUFFIX;
    public static final String READ_OPERATION_12_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery12.class.getSimpleName() + FREQUENCY_SUFFIX;
    public static final String READ_OPERATION_13_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery13.class.getSimpleName() + FREQUENCY_SUFFIX;
    public static final String READ_OPERATION_14_FREQUENCY_KEY =
            LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + LdbcQuery14.class.getSimpleName() + FREQUENCY_SUFFIX;
    public static final List<String> READ_OPERATION_FREQUENCY_KEYS = Lists.newArrayList(
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

    public static final Map<Integer,String> OPERATION_TYPE_TO_INTERLEAVE_KEY_MAPPING = typeToInterleaveKeyMapping();

    // Default value in case there is no update stream
    public static final String DEFAULT_UPDATE_INTERLEAVE = "1";

    /*
     * Operation Enable
     */
    public static final String ENABLE_SUFFIX = "_enable";
    public static final String LONG_READ_OPERATION_1_ENABLE_KEY = asEnableKey( LdbcQuery1.class );
    public static final String LONG_READ_OPERATION_2_ENABLE_KEY = asEnableKey( LdbcQuery2.class );
    public static final String LONG_READ_OPERATION_3_ENABLE_KEY = asEnableKey( LdbcQuery3.class );
    public static final String LONG_READ_OPERATION_4_ENABLE_KEY = asEnableKey( LdbcQuery4.class );
    public static final String LONG_READ_OPERATION_5_ENABLE_KEY = asEnableKey( LdbcQuery5.class );
    public static final String LONG_READ_OPERATION_6_ENABLE_KEY = asEnableKey( LdbcQuery6.class );
    public static final String LONG_READ_OPERATION_7_ENABLE_KEY = asEnableKey( LdbcQuery7.class );
    public static final String LONG_READ_OPERATION_8_ENABLE_KEY = asEnableKey( LdbcQuery8.class );
    public static final String LONG_READ_OPERATION_9_ENABLE_KEY = asEnableKey( LdbcQuery9.class );
    public static final String LONG_READ_OPERATION_10_ENABLE_KEY = asEnableKey( LdbcQuery10.class );
    public static final String LONG_READ_OPERATION_11_ENABLE_KEY = asEnableKey( LdbcQuery11.class );
    public static final String LONG_READ_OPERATION_12_ENABLE_KEY = asEnableKey( LdbcQuery12.class );
    public static final String LONG_READ_OPERATION_13_ENABLE_KEY = asEnableKey( LdbcQuery13.class );
    public static final String LONG_READ_OPERATION_14_ENABLE_KEY = asEnableKey( LdbcQuery14.class );
    public static final List<String> LONG_READ_OPERATION_ENABLE_KEYS = Lists.newArrayList(
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
    public static final String SHORT_READ_OPERATION_1_ENABLE_KEY = asEnableKey( LdbcShortQuery1PersonProfile.class );
    public static final String SHORT_READ_OPERATION_2_ENABLE_KEY = asEnableKey( LdbcShortQuery2PersonPosts.class );
    public static final String SHORT_READ_OPERATION_3_ENABLE_KEY = asEnableKey( LdbcShortQuery3PersonFriends.class );
    public static final String SHORT_READ_OPERATION_4_ENABLE_KEY = asEnableKey( LdbcShortQuery4MessageContent.class );
    public static final String SHORT_READ_OPERATION_5_ENABLE_KEY = asEnableKey( LdbcShortQuery5MessageCreator.class );
    public static final String SHORT_READ_OPERATION_6_ENABLE_KEY = asEnableKey( LdbcShortQuery6MessageForum.class );
    public static final String SHORT_READ_OPERATION_7_ENABLE_KEY = asEnableKey( LdbcShortQuery7MessageReplies.class );
    public static final List<String> SHORT_READ_OPERATION_ENABLE_KEYS = Lists.newArrayList(
            SHORT_READ_OPERATION_1_ENABLE_KEY,
            SHORT_READ_OPERATION_2_ENABLE_KEY,
            SHORT_READ_OPERATION_3_ENABLE_KEY,
            SHORT_READ_OPERATION_4_ENABLE_KEY,
            SHORT_READ_OPERATION_5_ENABLE_KEY,
            SHORT_READ_OPERATION_6_ENABLE_KEY,
            SHORT_READ_OPERATION_7_ENABLE_KEY
    );

    public static final String WRITE_OPERATION_1_ENABLE_KEY = asEnableKey( LdbcUpdate1AddPerson.class );
    public static final String WRITE_OPERATION_2_ENABLE_KEY = asEnableKey( LdbcUpdate2AddPostLike.class );
    public static final String WRITE_OPERATION_3_ENABLE_KEY = asEnableKey( LdbcUpdate3AddCommentLike.class );
    public static final String WRITE_OPERATION_4_ENABLE_KEY = asEnableKey( LdbcUpdate4AddForum.class );
    public static final String WRITE_OPERATION_5_ENABLE_KEY = asEnableKey( LdbcUpdate5AddForumMembership.class );
    public static final String WRITE_OPERATION_6_ENABLE_KEY = asEnableKey( LdbcUpdate6AddPost.class );
    public static final String WRITE_OPERATION_7_ENABLE_KEY = asEnableKey( LdbcUpdate7AddComment.class );
    public static final String WRITE_OPERATION_8_ENABLE_KEY = asEnableKey( LdbcUpdate8AddFriendship.class );
    public static final String WRITE_OPERATION_9_ENABLE_KEY = asEnableKey( LdbcDelete1RemovePerson.class );
    public static final String WRITE_OPERATION_10_ENABLE_KEY = asEnableKey( LdbcDelete2RemovePostLike.class );
    public static final String WRITE_OPERATION_11_ENABLE_KEY = asEnableKey( LdbcDelete3RemoveCommentLike.class );
    public static final String WRITE_OPERATION_12_ENABLE_KEY = asEnableKey( LdbcDelete4RemoveForum.class );
    public static final String WRITE_OPERATION_13_ENABLE_KEY = asEnableKey( LdbcDelete5RemoveForumMembership.class );
    public static final String WRITE_OPERATION_14_ENABLE_KEY = asEnableKey( LdbcDelete6RemovePostThread.class );
    public static final String WRITE_OPERATION_15_ENABLE_KEY = asEnableKey( LdbcDelete7RemoveCommentSubthread.class );
    public static final String WRITE_OPERATION_16_ENABLE_KEY = asEnableKey( LdbcDelete8RemoveFriendship.class );
    public static final List<String> WRITE_OPERATION_ENABLE_KEYS = Lists.newArrayList(
            WRITE_OPERATION_1_ENABLE_KEY, 
            WRITE_OPERATION_2_ENABLE_KEY,
            WRITE_OPERATION_3_ENABLE_KEY,
            WRITE_OPERATION_4_ENABLE_KEY,
            WRITE_OPERATION_5_ENABLE_KEY,
            WRITE_OPERATION_6_ENABLE_KEY,
            WRITE_OPERATION_7_ENABLE_KEY,
            WRITE_OPERATION_8_ENABLE_KEY
    );

    public static final List<String> DELETE_OPERATION_ENABLE_KEYS = Lists.newArrayList(
            WRITE_OPERATION_9_ENABLE_KEY,
            WRITE_OPERATION_10_ENABLE_KEY,
            WRITE_OPERATION_11_ENABLE_KEY,
            WRITE_OPERATION_12_ENABLE_KEY,
            WRITE_OPERATION_13_ENABLE_KEY,
            WRITE_OPERATION_14_ENABLE_KEY,
            WRITE_OPERATION_15_ENABLE_KEY,
            WRITE_OPERATION_16_ENABLE_KEY
    );
    
    private static String asEnableKey( Class<? extends Operation> operation )
    {
        return LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX + operation.getSimpleName() + ENABLE_SUFFIX;
    }

    /*
     * Read Operation Parameters
     */
    public static final String READ_OPERATION_1_PARAMS_FILENAME = "interactive-1.parquet";
    public static final String READ_OPERATION_2_PARAMS_FILENAME = "interactive-2.parquet";
    public static final String READ_OPERATION_3_PARAMS_FILENAME = "interactive-3.parquet";
    public static final String READ_OPERATION_4_PARAMS_FILENAME = "interactive-4.parquet";
    public static final String READ_OPERATION_5_PARAMS_FILENAME = "interactive-5.parquet";
    public static final String READ_OPERATION_6_PARAMS_FILENAME = "interactive-6.parquet";
    public static final String READ_OPERATION_7_PARAMS_FILENAME = "interactive-7.parquet";
    public static final String READ_OPERATION_8_PARAMS_FILENAME = "interactive-8.parquet";
    public static final String READ_OPERATION_9_PARAMS_FILENAME = "interactive-9.parquet";
    public static final String READ_OPERATION_10_PARAMS_FILENAME = "interactive-10.parquet";
    public static final String READ_OPERATION_11_PARAMS_FILENAME = "interactive-11.parquet";
    public static final String READ_OPERATION_12_PARAMS_FILENAME = "interactive-12.parquet";
    public static final String READ_OPERATION_13_PARAMS_FILENAME = "interactive-13.parquet";
    public static final String READ_OPERATION_14_PARAMS_FILENAME = "interactive-14.parquet";

    private static Map<Integer,String> typeToOperationParameterFilename()
    {
        Map<Integer,String> mapping = new HashMap<>();
        mapping.put( LdbcQuery1.TYPE, READ_OPERATION_1_PARAMS_FILENAME );
        mapping.put( LdbcQuery2.TYPE, READ_OPERATION_2_PARAMS_FILENAME );
        mapping.put( LdbcQuery3.TYPE, READ_OPERATION_3_PARAMS_FILENAME );
        mapping.put( LdbcQuery4.TYPE, READ_OPERATION_4_PARAMS_FILENAME );
        mapping.put( LdbcQuery5.TYPE, READ_OPERATION_5_PARAMS_FILENAME );
        mapping.put( LdbcQuery6.TYPE, READ_OPERATION_6_PARAMS_FILENAME );
        mapping.put( LdbcQuery7.TYPE, READ_OPERATION_7_PARAMS_FILENAME );
        mapping.put( LdbcQuery8.TYPE, READ_OPERATION_8_PARAMS_FILENAME );
        mapping.put( LdbcQuery9.TYPE, READ_OPERATION_9_PARAMS_FILENAME );
        mapping.put( LdbcQuery10.TYPE, READ_OPERATION_10_PARAMS_FILENAME );
        mapping.put( LdbcQuery11.TYPE, READ_OPERATION_11_PARAMS_FILENAME );
        mapping.put( LdbcQuery12.TYPE, READ_OPERATION_12_PARAMS_FILENAME );
        mapping.put( LdbcQuery13.TYPE, READ_OPERATION_13_PARAMS_FILENAME );
        mapping.put( LdbcQuery14.TYPE, READ_OPERATION_14_PARAMS_FILENAME );
        return mapping;
    }

    public static final Map<Integer, String> READ_OPERATION_PARAMS_FILENAMES = typeToOperationParameterFilename();

    /*
     * Write Operation Parameters
     */
    public static final String PIPE_SEPARATOR_REGEX = "\\|";

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

    public static Map<String,String> defaultConfigSF1() throws IOException
    {
        String filename = "/configuration/ldbc/snb/interactive/sf_internal_test.properties";
        return ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys( resourceToMap( filename ) );
    }

    private static Map<String,String> resourceToMap( String filename ) throws IOException
    {
        try ( InputStream inputStream = LdbcSnbInteractiveWorkloadConfiguration.class.getResource( filename ).openStream() )
        {
            Properties properties = new Properties();
            properties.load( inputStream );
            return new HashMap<>( Maps.fromProperties( properties ) );
        }
    }

    public static Map<String,String> defaultReadOnlyConfigSF1() throws DriverConfigurationException, IOException
    {
        Map<String,String> params = withoutWrites(
                defaultConfigSF1()
        );
        return ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys( params );
    }

    public static Map<String,String> withOnly(
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

    public static boolean hasReads( Map<String,String> params )
    {
        return Lists.newArrayList(
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
                LONG_READ_OPERATION_14_ENABLE_KEY ).stream().anyMatch( key -> isSet( params, key ) );
    }

    public static boolean hasWrites( Map<String,String> params )
    {
        return Lists.newArrayList(
                WRITE_OPERATION_1_ENABLE_KEY,
                WRITE_OPERATION_2_ENABLE_KEY,
                WRITE_OPERATION_3_ENABLE_KEY,
                WRITE_OPERATION_4_ENABLE_KEY,
                WRITE_OPERATION_5_ENABLE_KEY,
                WRITE_OPERATION_6_ENABLE_KEY,
                WRITE_OPERATION_7_ENABLE_KEY,
                WRITE_OPERATION_8_ENABLE_KEY,
                WRITE_OPERATION_9_ENABLE_KEY,
                WRITE_OPERATION_10_ENABLE_KEY,
                WRITE_OPERATION_11_ENABLE_KEY,
                WRITE_OPERATION_12_ENABLE_KEY,
                WRITE_OPERATION_13_ENABLE_KEY,
                WRITE_OPERATION_14_ENABLE_KEY,
                WRITE_OPERATION_15_ENABLE_KEY,
                WRITE_OPERATION_16_ENABLE_KEY
                ).stream().anyMatch( key -> isSet( params, key ) );
    }

    private static boolean isSet( Map<String,String> params, String key )
    {
        return params.containsKey( key ) &&
               null != params.get( key ) &&
               Boolean.parseBoolean( params.get( key ) );
    }

    public static Map<String,String> withoutShortReads( Map<String,String> originalParams )
            throws DriverConfigurationException, IOException
    {
        Map<String,String> params = MapUtils.copyExcludingKeys( originalParams, new HashSet<>() );
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
        Map<String,String> params = MapUtils.copyExcludingKeys( originalParams, new HashSet<>() );
        params.put( WRITE_OPERATION_1_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_2_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_3_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_4_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_5_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_6_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_7_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_8_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_9_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_10_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_11_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_12_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_13_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_14_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_15_ENABLE_KEY, "false" );
        params.put( WRITE_OPERATION_16_ENABLE_KEY, "false" );
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
        operationTypeToClassMapping.put( LdbcDelete1RemovePerson.TYPE, LdbcDelete1RemovePerson.class );
        operationTypeToClassMapping.put( LdbcDelete2RemovePostLike.TYPE, LdbcDelete2RemovePostLike.class );
        operationTypeToClassMapping.put( LdbcDelete3RemoveCommentLike.TYPE, LdbcDelete3RemoveCommentLike.class );
        operationTypeToClassMapping.put( LdbcDelete4RemoveForum.TYPE, LdbcDelete4RemoveForum.class );
        operationTypeToClassMapping.put( LdbcDelete5RemoveForumMembership.TYPE, LdbcDelete5RemoveForumMembership.class );
        operationTypeToClassMapping.put( LdbcDelete6RemovePostThread.TYPE, LdbcDelete6RemovePostThread.class );
        operationTypeToClassMapping.put( LdbcDelete7RemoveCommentSubthread.TYPE, LdbcDelete7RemoveCommentSubthread.class );
        operationTypeToClassMapping.put( LdbcDelete8RemoveFriendship.TYPE, LdbcDelete8RemoveFriendship.class );
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

    /**
     * Get mapping of update/delete operation and filename containing the events
     */
    public static Map<Class<? extends Operation>, String> getUpdateStreamClassToPathMapping( )
    {
        Map<Class<? extends Operation>, String> classToFileNameMapping = new HashMap<>();
        // Inserts
        classToFileNameMapping.put( LdbcUpdate1AddPerson.class, INSERTS_DIRECTORY + "/Person.parquet" );
        classToFileNameMapping.put( LdbcUpdate2AddPostLike.class, INSERTS_DIRECTORY + "/Person_likes_Post.parquet" );
        classToFileNameMapping.put( LdbcUpdate3AddCommentLike.class, INSERTS_DIRECTORY + "/Person_likes_Comment.parquet" );
        classToFileNameMapping.put( LdbcUpdate4AddForum.class, INSERTS_DIRECTORY + "/Forum.parquet" );
        classToFileNameMapping.put( LdbcUpdate5AddForumMembership.class, INSERTS_DIRECTORY + "/Forum_hasMember_Person.parquet" );
        classToFileNameMapping.put( LdbcUpdate6AddPost.class, INSERTS_DIRECTORY + "/Post.parquet" );
        classToFileNameMapping.put( LdbcUpdate7AddComment.class, INSERTS_DIRECTORY + "/Comment.parquet" );
        classToFileNameMapping.put( LdbcUpdate8AddFriendship.class, INSERTS_DIRECTORY + "/Person_knows_Person.parquet" );

        // Deletes
        classToFileNameMapping.put( LdbcDelete1RemovePerson.class, DELETES_DIRECTORY + "/Person.parquet" );
        classToFileNameMapping.put( LdbcDelete2RemovePostLike.class, DELETES_DIRECTORY + "/Person_likes_Post.parquet" );
        classToFileNameMapping.put( LdbcDelete3RemoveCommentLike.class, DELETES_DIRECTORY + "/Person_likes_Comment.parquet" );
        classToFileNameMapping.put( LdbcDelete4RemoveForum.class, DELETES_DIRECTORY + "/Forum.parquet" );
        classToFileNameMapping.put( LdbcDelete5RemoveForumMembership.class, DELETES_DIRECTORY + "/Forum_hasMember_Person.parquet" );
        classToFileNameMapping.put( LdbcDelete6RemovePostThread.class, DELETES_DIRECTORY + "/Post.parquet" );
        classToFileNameMapping.put( LdbcDelete7RemoveCommentSubthread.class, DELETES_DIRECTORY + "/Comment.parquet" );
        classToFileNameMapping.put( LdbcDelete8RemoveFriendship.class, DELETES_DIRECTORY + "/Person_knows_Person.parquet" );
        return classToFileNameMapping;
    }
}
