package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.ClassLoadingException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.ldbc.driver.util.FileUtils.removePrefix;
import static com.ldbc.driver.util.FileUtils.removeSuffix;
import static java.lang.String.format;

public class LdbcSnbBiWorkloadConfiguration
{
    public final static String LDBC_SNB_BI_PARAM_NAME_PREFIX = "ldbc.snb.bi.";
    // directory that contains the substitution parameters files
    public final static String PARAMETERS_DIRECTORY = LDBC_SNB_BI_PARAM_NAME_PREFIX + "parameters_dir";
    // TODO this should be private and conversion to class names should be done by this class
    private final static String LDBC_SNB_BI_PACKAGE_PREFIX = removeSuffix(
            LdbcSnbBiWorkloadConfiguration.class.getName(), LdbcSnbBiWorkloadConfiguration.class.getSimpleName()
    );

    /*
     * Operation frequency
     */
    public final static String FREQUENCY_SUFFIX = "_freq";
    public final static String OPERATION_1_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery1PostingSummary.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_2_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery2TagEvolution.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_3_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery3PopularCountryTopics.class.getSimpleName() +
            FREQUENCY_SUFFIX;
    public final static String OPERATION_4_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery4TopCountryPosters.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_5_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery5ActivePosters.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_6_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery6AuthoritativeUsers.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_7_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery7RelatedTopics.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_8_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery8TagPerson.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_9_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery9TopThreadInitiators.class.getSimpleName() +
            FREQUENCY_SUFFIX;
    public final static String OPERATION_10_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery10ExpertsInSocialCircle.class.getSimpleName() +
            FREQUENCY_SUFFIX;
    public final static String OPERATION_11_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery11FriendshipTriangles.class.getSimpleName() +
            FREQUENCY_SUFFIX;
    public final static String OPERATION_12_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery12PersonPostCounts.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_13_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery13Zombies.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_14_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery14InternationalDialog.class.getSimpleName() +
            FREQUENCY_SUFFIX;
    public final static String OPERATION_15_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery15WeightedPaths.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static List<String> OPERATION_FREQUENCY_KEYS = Lists.newArrayList(
            OPERATION_1_FREQUENCY_KEY,
            OPERATION_2_FREQUENCY_KEY,
            OPERATION_3_FREQUENCY_KEY,
            OPERATION_4_FREQUENCY_KEY,
            OPERATION_5_FREQUENCY_KEY,
            OPERATION_6_FREQUENCY_KEY,
            OPERATION_7_FREQUENCY_KEY,
            OPERATION_8_FREQUENCY_KEY,
            OPERATION_9_FREQUENCY_KEY,
            OPERATION_10_FREQUENCY_KEY,
            OPERATION_11_FREQUENCY_KEY,
            OPERATION_12_FREQUENCY_KEY,
            OPERATION_13_FREQUENCY_KEY,
            OPERATION_14_FREQUENCY_KEY,
            OPERATION_15_FREQUENCY_KEY
    );

    private static Map<Integer,String> typeToFrequencyKeyMapping()
    {
        Map<Integer,String> mapping = new HashMap<>();
        mapping.put( LdbcSnbBiQuery1PostingSummary.TYPE, OPERATION_1_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery2TagEvolution.TYPE, OPERATION_2_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery3PopularCountryTopics.TYPE, OPERATION_3_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery4TopCountryPosters.TYPE, OPERATION_4_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery5ActivePosters.TYPE, OPERATION_5_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery6AuthoritativeUsers.TYPE, OPERATION_6_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery7RelatedTopics.TYPE, OPERATION_7_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery8TagPerson.TYPE, OPERATION_8_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery9TopThreadInitiators.TYPE, OPERATION_9_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery10ExpertsInSocialCircle.TYPE, OPERATION_10_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery11FriendshipTriangles.TYPE, OPERATION_11_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery12PersonPostCounts.TYPE, OPERATION_12_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery13Zombies.TYPE, OPERATION_13_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery14InternationalDialog.TYPE, OPERATION_14_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery15WeightedPaths.TYPE, OPERATION_15_FREQUENCY_KEY );
        return mapping;
    }

    public final static Map<Integer,String> OPERATION_TYPE_TO_FREQUENCY_KEY_MAPPING = typeToFrequencyKeyMapping();

    /*
    * Operation Interleave
    */
    public final static String INTERLEAVE_SUFFIX = "_interleave";
    public final static String OPERATION_1_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery1PostingSummary.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_2_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery2TagEvolution.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_3_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery3PopularCountryTopics.class.getSimpleName() +
            INTERLEAVE_SUFFIX;
    public final static String OPERATION_4_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery4TopCountryPosters.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_5_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery5ActivePosters.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_6_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery6AuthoritativeUsers.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_7_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery7RelatedTopics.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_8_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery8TagPerson.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_9_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery9TopThreadInitiators.class.getSimpleName() +
            INTERLEAVE_SUFFIX;
    public final static String OPERATION_10_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery10ExpertsInSocialCircle.class.getSimpleName() +
            INTERLEAVE_SUFFIX;
    public final static String OPERATION_11_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery11FriendshipTriangles.class.getSimpleName() +
            INTERLEAVE_SUFFIX;
    public final static String OPERATION_12_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery12PersonPostCounts.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_13_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery13Zombies.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_14_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery14InternationalDialog.class.getSimpleName() +
            INTERLEAVE_SUFFIX;
    public final static String OPERATION_15_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery15WeightedPaths.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static List<String> OPERATION_INTERLEAVE_KEYS = Lists.newArrayList(
            OPERATION_1_INTERLEAVE_KEY,
            OPERATION_2_INTERLEAVE_KEY,
            OPERATION_3_INTERLEAVE_KEY,
            OPERATION_4_INTERLEAVE_KEY,
            OPERATION_5_INTERLEAVE_KEY,
            OPERATION_6_INTERLEAVE_KEY,
            OPERATION_7_INTERLEAVE_KEY,
            OPERATION_8_INTERLEAVE_KEY,
            OPERATION_9_INTERLEAVE_KEY,
            OPERATION_10_INTERLEAVE_KEY,
            OPERATION_11_INTERLEAVE_KEY,
            OPERATION_12_INTERLEAVE_KEY,
            OPERATION_13_INTERLEAVE_KEY,
            OPERATION_14_INTERLEAVE_KEY,
            OPERATION_15_INTERLEAVE_KEY
    );

    private static Map<Integer,String> typeToInterleaveKeyMapping()
    {
        Map<Integer,String> mapping = new HashMap<>();
        mapping.put( LdbcSnbBiQuery1PostingSummary.TYPE, OPERATION_1_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery2TagEvolution.TYPE, OPERATION_2_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery3PopularCountryTopics.TYPE, OPERATION_3_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery4TopCountryPosters.TYPE, OPERATION_4_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery5ActivePosters.TYPE, OPERATION_5_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery6AuthoritativeUsers.TYPE, OPERATION_6_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery7RelatedTopics.TYPE, OPERATION_7_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery8TagPerson.TYPE, OPERATION_8_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery9TopThreadInitiators.TYPE, OPERATION_9_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery10ExpertsInSocialCircle.TYPE, OPERATION_10_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery11FriendshipTriangles.TYPE, OPERATION_11_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery12PersonPostCounts.TYPE, OPERATION_12_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery13Zombies.TYPE, OPERATION_13_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery14InternationalDialog.TYPE, OPERATION_14_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery15WeightedPaths.TYPE, OPERATION_15_INTERLEAVE_KEY );
        return mapping;
    }

    public final static Map<Integer,String> OPERATION_TYPE_TO_INTERLEAVE_KEY_MAPPING = typeToInterleaveKeyMapping();

    /*
     * Operation Enable
     */
    public final static String ENABLE_SUFFIX = "_enable";
    public final static String OPERATION_1_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery1PostingSummary.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_2_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery2TagEvolution.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_3_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery3PopularCountryTopics.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_4_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery4TopCountryPosters.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_5_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery5ActivePosters.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_6_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery6AuthoritativeUsers.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_7_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery7RelatedTopics.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_8_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery8TagPerson.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_9_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery9TopThreadInitiators.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_10_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery10ExpertsInSocialCircle.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_11_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery11FriendshipTriangles.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_12_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery12PersonPostCounts.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_13_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery13Zombies.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_14_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery14InternationalDialog.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_15_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery15WeightedPaths.class.getSimpleName() + ENABLE_SUFFIX;
    public final static List<String> OPERATION_ENABLE_KEYS = Lists.newArrayList(
            OPERATION_1_ENABLE_KEY,
            OPERATION_2_ENABLE_KEY,
            OPERATION_3_ENABLE_KEY,
            OPERATION_4_ENABLE_KEY,
            OPERATION_5_ENABLE_KEY,
            OPERATION_6_ENABLE_KEY,
            OPERATION_7_ENABLE_KEY,
            OPERATION_8_ENABLE_KEY,
            OPERATION_9_ENABLE_KEY,
            OPERATION_10_ENABLE_KEY,
            OPERATION_11_ENABLE_KEY,
            OPERATION_12_ENABLE_KEY,
            OPERATION_13_ENABLE_KEY,
            OPERATION_14_ENABLE_KEY,
            OPERATION_15_ENABLE_KEY
    );

    /*
     * Read Operation Parameters
     */
    public final static String OPERATION_1_PARAMS_FILENAME = "bi_1_param.txt";
    public final static String OPERATION_2_PARAMS_FILENAME = "bi_2_param.txt";
    public final static String OPERATION_3_PARAMS_FILENAME = "bi_3_param.txt";
    public final static String OPERATION_4_PARAMS_FILENAME = "bi_4_param.txt";
    public final static String OPERATION_5_PARAMS_FILENAME = "bi_5_param.txt";
    public final static String OPERATION_6_PARAMS_FILENAME = "bi_6_param.txt";
    public final static String OPERATION_7_PARAMS_FILENAME = "bi_7_param.txt";
    public final static String OPERATION_8_PARAMS_FILENAME = "bi_8_param.txt";
    public final static String OPERATION_9_PARAMS_FILENAME = "bi_9_param.txt";
    public final static String OPERATION_10_PARAMS_FILENAME = "bi_10_param.txt";
    public final static String OPERATION_11_PARAMS_FILENAME = "bi_11_param.txt";
    public final static String OPERATION_12_PARAMS_FILENAME = "bi_12_param.txt";
    public final static String OPERATION_13_PARAMS_FILENAME = "bi_13_param.txt";
    public final static String OPERATION_14_PARAMS_FILENAME = "bi_14_param.txt";
    public final static String OPERATION_15_PARAMS_FILENAME = "bi_15_param.txt";
    public final static List<String> OPERATION_PARAMS_FILENAMES = Lists.newArrayList(
            OPERATION_1_PARAMS_FILENAME,
            OPERATION_2_PARAMS_FILENAME,
            OPERATION_3_PARAMS_FILENAME,
            OPERATION_4_PARAMS_FILENAME,
            OPERATION_5_PARAMS_FILENAME,
            OPERATION_6_PARAMS_FILENAME,
            OPERATION_7_PARAMS_FILENAME,
            OPERATION_8_PARAMS_FILENAME,
            OPERATION_9_PARAMS_FILENAME,
            OPERATION_10_PARAMS_FILENAME,
            OPERATION_11_PARAMS_FILENAME,
            OPERATION_12_PARAMS_FILENAME,
            OPERATION_13_PARAMS_FILENAME,
            OPERATION_14_PARAMS_FILENAME,
            OPERATION_15_PARAMS_FILENAME
    );

    public static Map<String,String> applyInterleaves( Map<String,String> params, LdbcSnbBiInterleaves interleaves )
    {
        Map<String,String> newParams = new HashMap<>();
        for ( String paramKey : params.keySet() )
        {
            newParams.put( paramKey, params.get( paramKey ) );
        }
        newParams.put( OPERATION_1_INTERLEAVE_KEY, Long.toString( interleaves.operation1Interleave ) );
        newParams.put( OPERATION_2_INTERLEAVE_KEY, Long.toString( interleaves.operation2Interleave ) );
        newParams.put( OPERATION_3_INTERLEAVE_KEY, Long.toString( interleaves.operation3Interleave ) );
        newParams.put( OPERATION_4_INTERLEAVE_KEY, Long.toString( interleaves.operation4Interleave ) );
        newParams.put( OPERATION_5_INTERLEAVE_KEY, Long.toString( interleaves.operation5Interleave ) );
        newParams.put( OPERATION_6_INTERLEAVE_KEY, Long.toString( interleaves.operation6Interleave ) );
        newParams.put( OPERATION_7_INTERLEAVE_KEY, Long.toString( interleaves.operation7Interleave ) );
        newParams.put( OPERATION_8_INTERLEAVE_KEY, Long.toString( interleaves.operation8Interleave ) );
        newParams.put( OPERATION_9_INTERLEAVE_KEY, Long.toString( interleaves.operation9Interleave ) );
        newParams.put( OPERATION_10_INTERLEAVE_KEY, Long.toString( interleaves.operation10Interleave ) );
        newParams.put( OPERATION_11_INTERLEAVE_KEY, Long.toString( interleaves.operation11Interleave ) );
        newParams.put( OPERATION_12_INTERLEAVE_KEY, Long.toString( interleaves.operation12Interleave ) );
        newParams.put( OPERATION_13_INTERLEAVE_KEY, Long.toString( interleaves.operation13Interleave ) );
        newParams.put( OPERATION_14_INTERLEAVE_KEY, Long.toString( interleaves.operation14Interleave ) );
        newParams.put( OPERATION_15_INTERLEAVE_KEY, Long.toString( interleaves.operation15Interleave ) );
        return newParams;
    }

    public static LdbcSnbBiInterleaves interleavesFromFrequencyParams( Map<String,String> params )
    {
        // TODO revise/replace
        Integer minimumDistanceAsMilli = 1;
        return LdbcSnbBiInterleaves.fromFrequencies(
                minimumDistanceAsMilli,
                Long.parseLong( params.get( OPERATION_1_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_2_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_3_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_4_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_5_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_6_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_7_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_8_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_9_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_10_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_11_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_12_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_13_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_14_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_15_FREQUENCY_KEY ) )
        );
    }

    public static LdbcSnbBiInterleaves interleavesFromInterleaveParams(
            Map<String,String> params )
    {
        return new LdbcSnbBiInterleaves(
                Long.parseLong( params.get( OPERATION_1_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_2_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_3_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_4_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_5_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_6_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_7_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_8_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_9_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_10_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_11_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_12_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_13_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_14_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_15_INTERLEAVE_KEY ) )
        );
    }

    static class LdbcSnbBiInterleaves
    {
        final long operation1Interleave;
        final long operation2Interleave;
        final long operation3Interleave;
        final long operation4Interleave;
        final long operation5Interleave;
        final long operation6Interleave;
        final long operation7Interleave;
        final long operation8Interleave;
        final long operation9Interleave;
        final long operation10Interleave;
        final long operation11Interleave;
        final long operation12Interleave;
        final long operation13Interleave;
        final long operation14Interleave;
        final long operation15Interleave;

        static LdbcSnbBiInterleaves fromFrequencies(
                long minimumDistanceAsMilli,
                long operation1Frequency,
                long operation2Frequency,
                long operation3Frequency,
                long operation4Frequency,
                long operation5Frequency,
                long operation6Frequency,
                long operation7Frequency,
                long operation8Frequency,
                long operation9Frequency,
                long operation10Frequency,
                long operation11Frequency,
                long operation12Frequency,
                long operation13Frequency,
                long operation14Frequency,
                long operation15Frequency )
        {
            return new LdbcSnbBiInterleaves(
                    operation1Frequency * minimumDistanceAsMilli,
                    operation2Frequency * minimumDistanceAsMilli,
                    operation3Frequency * minimumDistanceAsMilli,
                    operation4Frequency * minimumDistanceAsMilli,
                    operation5Frequency * minimumDistanceAsMilli,
                    operation6Frequency * minimumDistanceAsMilli,
                    operation7Frequency * minimumDistanceAsMilli,
                    operation8Frequency * minimumDistanceAsMilli,
                    operation9Frequency * minimumDistanceAsMilli,
                    operation10Frequency * minimumDistanceAsMilli,
                    operation11Frequency * minimumDistanceAsMilli,
                    operation12Frequency * minimumDistanceAsMilli,
                    operation13Frequency * minimumDistanceAsMilli,
                    operation14Frequency * minimumDistanceAsMilli,
                    operation15Frequency * minimumDistanceAsMilli
            );
        }

        private LdbcSnbBiInterleaves(
                long operation1Interleave,
                long operation2Interleave,
                long operation3Interleave,
                long operation4Interleave,
                long operation5Interleave,
                long operation6Interleave,
                long operation7Interleave,
                long operation8Interleave,
                long operation9Interleave,
                long operation10Interleave,
                long operation11Interleave,
                long operation12Interleave,
                long operation13Interleave,
                long operation14Interleave,
                long operation15Interleave )
        {
            this.operation1Interleave = operation1Interleave;
            this.operation2Interleave = operation2Interleave;
            this.operation3Interleave = operation3Interleave;
            this.operation4Interleave = operation4Interleave;
            this.operation5Interleave = operation5Interleave;
            this.operation6Interleave = operation6Interleave;
            this.operation7Interleave = operation7Interleave;
            this.operation8Interleave = operation8Interleave;
            this.operation9Interleave = operation9Interleave;
            this.operation10Interleave = operation10Interleave;
            this.operation11Interleave = operation11Interleave;
            this.operation12Interleave = operation12Interleave;
            this.operation13Interleave = operation13Interleave;
            this.operation14Interleave = operation14Interleave;
            this.operation15Interleave = operation15Interleave;
        }
    }

    public static Map<String,String> defaultConfigSF1() throws IOException
    {
        String filename = "/configuration/ldbc/snb/bi/ldbc_snb_bi_SF-0001.properties";
        return ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys(
                resourceToMap( filename )
        );
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

    public static Map<Integer,Class<? extends Operation>> operationTypeToClassMapping()
    {
        Map<Integer,Class<? extends Operation>> mapping = new HashMap<>();
        mapping.put( LdbcSnbBiQuery1PostingSummary.TYPE, LdbcSnbBiQuery1PostingSummary.class );
        mapping.put( LdbcSnbBiQuery2TagEvolution.TYPE, LdbcSnbBiQuery2TagEvolution.class );
        mapping.put( LdbcSnbBiQuery3PopularCountryTopics.TYPE, LdbcSnbBiQuery3PopularCountryTopics.class );
        mapping.put( LdbcSnbBiQuery4TopCountryPosters.TYPE, LdbcSnbBiQuery4TopCountryPosters.class );
        mapping.put( LdbcSnbBiQuery5ActivePosters.TYPE, LdbcSnbBiQuery5ActivePosters.class );
        mapping.put( LdbcSnbBiQuery6AuthoritativeUsers.TYPE, LdbcSnbBiQuery6AuthoritativeUsers.class );
        mapping.put( LdbcSnbBiQuery7RelatedTopics.TYPE, LdbcSnbBiQuery7RelatedTopics.class );
        mapping.put( LdbcSnbBiQuery8TagPerson.TYPE, LdbcSnbBiQuery8TagPerson.class );
        mapping.put( LdbcSnbBiQuery9TopThreadInitiators.TYPE, LdbcSnbBiQuery9TopThreadInitiators.class );
        mapping.put( LdbcSnbBiQuery10ExpertsInSocialCircle.TYPE, LdbcSnbBiQuery10ExpertsInSocialCircle.class );
        mapping.put( LdbcSnbBiQuery11FriendshipTriangles.TYPE, LdbcSnbBiQuery11FriendshipTriangles.class );
        mapping.put( LdbcSnbBiQuery12PersonPostCounts.TYPE, LdbcSnbBiQuery12PersonPostCounts.class );
        mapping.put( LdbcSnbBiQuery13Zombies.TYPE, LdbcSnbBiQuery13Zombies.class );
        mapping.put( LdbcSnbBiQuery14InternationalDialog.TYPE, LdbcSnbBiQuery14InternationalDialog.class );
        mapping.put( LdbcSnbBiQuery15WeightedPaths.TYPE, LdbcSnbBiQuery15WeightedPaths.class );
        return mapping;
    }

    static Set<String> missingParameters( Map<String,String> parameters, Iterable<String> compulsoryParameterKeys )
    {
        Set<String> missingPropertyKeys = new HashSet<>();
        for ( String compulsoryKey : compulsoryParameterKeys )
        {
            if ( null == parameters.get( compulsoryKey ) )
            { missingPropertyKeys.add( compulsoryKey ); }
        }
        return missingPropertyKeys;
    }

    static Class operationEnabledKeyToClass( String operationEnableKey ) throws WorkloadException
    {
        String operationClassName = LDBC_SNB_BI_PACKAGE_PREFIX +
                                    removePrefix(
                                            removeSuffix( operationEnableKey, ENABLE_SUFFIX ),
                                            LDBC_SNB_BI_PARAM_NAME_PREFIX
                                    );
        try
        {
            return ClassLoaderHelper.loadClass( operationClassName );
        }
        catch ( ClassLoadingException e )
        {
            throw new WorkloadException(
                    format( "Error loading operation class for parameter: %s\nGuessed class name: %s",
                            operationEnableKey,
                            operationClassName ),
                    e );
        }
    }

    public static boolean hasReads( Map<String,String> params )
    {
        return params.containsKey( OPERATION_1_ENABLE_KEY ) ||
               params.containsKey( OPERATION_2_ENABLE_KEY ) ||
               params.containsKey( OPERATION_3_ENABLE_KEY ) ||
               params.containsKey( OPERATION_4_ENABLE_KEY ) ||
               params.containsKey( OPERATION_5_ENABLE_KEY ) ||
               params.containsKey( OPERATION_6_ENABLE_KEY ) ||
               params.containsKey( OPERATION_7_ENABLE_KEY ) ||
               params.containsKey( OPERATION_8_ENABLE_KEY ) ||
               params.containsKey( OPERATION_9_ENABLE_KEY ) ||
               params.containsKey( OPERATION_10_ENABLE_KEY ) ||
               params.containsKey( OPERATION_11_ENABLE_KEY ) ||
               params.containsKey( OPERATION_12_ENABLE_KEY ) ||
               params.containsKey( OPERATION_13_ENABLE_KEY ) ||
               params.containsKey( OPERATION_14_ENABLE_KEY ) ||
               params.containsKey( OPERATION_15_ENABLE_KEY );
    }

    public static boolean hasWrites( Map<String,String> params )
    {
        return false;
    }
}
