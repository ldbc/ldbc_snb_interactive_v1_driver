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
    public final static String OPERATION_3_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery3TagEvolution.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_4_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery4PopularCountryTopics.class.getSimpleName() +
            FREQUENCY_SUFFIX;
    public final static String OPERATION_5_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery5TopCountryPosters.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_6_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery6ActivePosters.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_7_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery7AuthoritativeUsers.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_8_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery8RelatedTopics.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_10_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery10TagPerson.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_14_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery14TopThreadInitiators.class.getSimpleName() +
            FREQUENCY_SUFFIX;
    public final static String OPERATION_16_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery16ExpertsInSocialCircle.class.getSimpleName() +
            FREQUENCY_SUFFIX;
    public final static String OPERATION_17_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery17FriendshipTriangles.class.getSimpleName() +
            FREQUENCY_SUFFIX;
    public final static String OPERATION_18_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery18PersonPostCounts.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_21_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery21Zombies.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_22_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery22InternationalDialog.class.getSimpleName() +
            FREQUENCY_SUFFIX;
    public final static String OPERATION_25_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery25WeightedPaths.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static List<String> OPERATION_FREQUENCY_KEYS = Lists.newArrayList(
            OPERATION_1_FREQUENCY_KEY,
            OPERATION_3_FREQUENCY_KEY,
            OPERATION_4_FREQUENCY_KEY,
            OPERATION_5_FREQUENCY_KEY,
            OPERATION_6_FREQUENCY_KEY,
            OPERATION_7_FREQUENCY_KEY,
            OPERATION_8_FREQUENCY_KEY,
            OPERATION_10_FREQUENCY_KEY,
            OPERATION_14_FREQUENCY_KEY,
            OPERATION_16_FREQUENCY_KEY,
            OPERATION_17_FREQUENCY_KEY,
            OPERATION_18_FREQUENCY_KEY,
            OPERATION_21_FREQUENCY_KEY,
            OPERATION_22_FREQUENCY_KEY,
            OPERATION_25_FREQUENCY_KEY
    );

    private static Map<Integer,String> typeToFrequencyKeyMapping()
    {
        Map<Integer,String> mapping = new HashMap<>();
        mapping.put( LdbcSnbBiQuery1PostingSummary.TYPE, OPERATION_1_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery3TagEvolution.TYPE, OPERATION_3_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery4PopularCountryTopics.TYPE, OPERATION_4_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery5TopCountryPosters.TYPE, OPERATION_5_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery6ActivePosters.TYPE, OPERATION_6_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery7AuthoritativeUsers.TYPE, OPERATION_7_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery8RelatedTopics.TYPE, OPERATION_8_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery10TagPerson.TYPE, OPERATION_10_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery14TopThreadInitiators.TYPE, OPERATION_14_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery16ExpertsInSocialCircle.TYPE, OPERATION_16_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery17FriendshipTriangles.TYPE, OPERATION_17_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery18PersonPostCounts.TYPE, OPERATION_18_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery21Zombies.TYPE, OPERATION_21_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery22InternationalDialog.TYPE, OPERATION_22_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery25WeightedPaths.TYPE, OPERATION_25_FREQUENCY_KEY );
        return mapping;
    }

    public final static Map<Integer,String> OPERATION_TYPE_TO_FREQUENCY_KEY_MAPPING = typeToFrequencyKeyMapping();

    /*
    * Operation Interleave
    */
    public final static String INTERLEAVE_SUFFIX = "_interleave";
    public final static String OPERATION_1_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery1PostingSummary.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_3_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery3TagEvolution.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_4_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery4PopularCountryTopics.class.getSimpleName() +
            INTERLEAVE_SUFFIX;
    public final static String OPERATION_5_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery5TopCountryPosters.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_6_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery6ActivePosters.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_7_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery7AuthoritativeUsers.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_8_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery8RelatedTopics.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_10_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery10TagPerson.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_14_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery14TopThreadInitiators.class.getSimpleName() +
            INTERLEAVE_SUFFIX;
    public final static String OPERATION_16_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery16ExpertsInSocialCircle.class.getSimpleName() +
            INTERLEAVE_SUFFIX;
    public final static String OPERATION_17_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery17FriendshipTriangles.class.getSimpleName() +
            INTERLEAVE_SUFFIX;
    public final static String OPERATION_18_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery18PersonPostCounts.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_21_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery21Zombies.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_22_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery22InternationalDialog.class.getSimpleName() +
            INTERLEAVE_SUFFIX;
    public final static String OPERATION_25_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery25WeightedPaths.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static List<String> OPERATION_INTERLEAVE_KEYS = Lists.newArrayList(
            OPERATION_1_INTERLEAVE_KEY,
            OPERATION_3_INTERLEAVE_KEY,
            OPERATION_4_INTERLEAVE_KEY,
            OPERATION_5_INTERLEAVE_KEY,
            OPERATION_6_INTERLEAVE_KEY,
            OPERATION_7_INTERLEAVE_KEY,
            OPERATION_8_INTERLEAVE_KEY,
            OPERATION_10_INTERLEAVE_KEY,
            OPERATION_14_INTERLEAVE_KEY,
            OPERATION_16_INTERLEAVE_KEY,
            OPERATION_17_INTERLEAVE_KEY,
            OPERATION_18_INTERLEAVE_KEY,
            OPERATION_21_INTERLEAVE_KEY,
            OPERATION_22_INTERLEAVE_KEY,
            OPERATION_25_INTERLEAVE_KEY
    );

    private static Map<Integer,String> typeToInterleaveKeyMapping()
    {
        Map<Integer,String> mapping = new HashMap<>();
        mapping.put( LdbcSnbBiQuery1PostingSummary.TYPE, OPERATION_1_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery3TagEvolution.TYPE, OPERATION_3_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery4PopularCountryTopics.TYPE, OPERATION_4_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery5TopCountryPosters.TYPE, OPERATION_5_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery6ActivePosters.TYPE, OPERATION_6_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery7AuthoritativeUsers.TYPE, OPERATION_7_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery8RelatedTopics.TYPE, OPERATION_8_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery10TagPerson.TYPE, OPERATION_10_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery14TopThreadInitiators.TYPE, OPERATION_14_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery16ExpertsInSocialCircle.TYPE, OPERATION_16_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery17FriendshipTriangles.TYPE, OPERATION_17_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery18PersonPostCounts.TYPE, OPERATION_18_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery21Zombies.TYPE, OPERATION_21_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery22InternationalDialog.TYPE, OPERATION_22_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery25WeightedPaths.TYPE, OPERATION_25_INTERLEAVE_KEY );
        return mapping;
    }

    public final static Map<Integer,String> OPERATION_TYPE_TO_INTERLEAVE_KEY_MAPPING = typeToInterleaveKeyMapping();

    /*
     * Operation Enable
     */
    public final static String ENABLE_SUFFIX = "_enable";
    public final static String OPERATION_1_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery1PostingSummary.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_3_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery3TagEvolution.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_4_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery4PopularCountryTopics.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_5_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery5TopCountryPosters.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_6_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery6ActivePosters.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_7_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery7AuthoritativeUsers.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_8_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery8RelatedTopics.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_10_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery10TagPerson.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_14_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery14TopThreadInitiators.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_16_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery16ExpertsInSocialCircle.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_17_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery17FriendshipTriangles.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_18_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery18PersonPostCounts.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_21_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery21Zombies.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_22_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery22InternationalDialog.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_25_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery25WeightedPaths.class.getSimpleName() + ENABLE_SUFFIX;
    public final static List<String> OPERATION_ENABLE_KEYS = Lists.newArrayList(
            OPERATION_1_ENABLE_KEY,
            OPERATION_3_ENABLE_KEY,
            OPERATION_4_ENABLE_KEY,
            OPERATION_5_ENABLE_KEY,
            OPERATION_6_ENABLE_KEY,
            OPERATION_7_ENABLE_KEY,
            OPERATION_8_ENABLE_KEY,
            OPERATION_10_ENABLE_KEY,
            OPERATION_14_ENABLE_KEY,
            OPERATION_16_ENABLE_KEY,
            OPERATION_17_ENABLE_KEY,
            OPERATION_18_ENABLE_KEY,
            OPERATION_21_ENABLE_KEY,
            OPERATION_22_ENABLE_KEY,
            OPERATION_25_ENABLE_KEY
    );

    /*
     * Read Operation Parameters
     */
    public final static String OPERATION_1_PARAMS_FILENAME = "bi_1_param.txt";
    public final static String OPERATION_3_PARAMS_FILENAME = "bi_3_param.txt";
    public final static String OPERATION_4_PARAMS_FILENAME = "bi_4_param.txt";
    public final static String OPERATION_5_PARAMS_FILENAME = "bi_5_param.txt";
    public final static String OPERATION_6_PARAMS_FILENAME = "bi_6_param.txt";
    public final static String OPERATION_7_PARAMS_FILENAME = "bi_7_param.txt";
    public final static String OPERATION_8_PARAMS_FILENAME = "bi_8_param.txt";
    public final static String OPERATION_10_PARAMS_FILENAME = "bi_10_param.txt";
    public final static String OPERATION_14_PARAMS_FILENAME = "bi_14_param.txt";
    public final static String OPERATION_16_PARAMS_FILENAME = "bi_16_param.txt";
    public final static String OPERATION_17_PARAMS_FILENAME = "bi_17_param.txt";
    public final static String OPERATION_18_PARAMS_FILENAME = "bi_18_param.txt";
    public final static String OPERATION_21_PARAMS_FILENAME = "bi_21_param.txt";
    public final static String OPERATION_22_PARAMS_FILENAME = "bi_22_param.txt";
    public final static String OPERATION_25_PARAMS_FILENAME = "bi_25_param.txt";
    public final static List<String> OPERATION_PARAMS_FILENAMES = Lists.newArrayList(
            OPERATION_1_PARAMS_FILENAME,
            OPERATION_3_PARAMS_FILENAME,
            OPERATION_4_PARAMS_FILENAME,
            OPERATION_5_PARAMS_FILENAME,
            OPERATION_6_PARAMS_FILENAME,
            OPERATION_7_PARAMS_FILENAME,
            OPERATION_8_PARAMS_FILENAME,
            OPERATION_10_PARAMS_FILENAME,
            OPERATION_14_PARAMS_FILENAME,
            OPERATION_16_PARAMS_FILENAME,
            OPERATION_17_PARAMS_FILENAME,
            OPERATION_18_PARAMS_FILENAME,
            OPERATION_21_PARAMS_FILENAME,
            OPERATION_22_PARAMS_FILENAME,
            OPERATION_25_PARAMS_FILENAME
    );

    public static Map<String,String> applyInterleaves( Map<String,String> params, LdbcSnbBiInterleaves interleaves )
    {
        Map<String,String> newParams = new HashMap<>();
        for ( String paramKey : params.keySet() )
        {
            newParams.put( paramKey, params.get( paramKey ) );
        }
        newParams.put( OPERATION_1_INTERLEAVE_KEY, Long.toString( interleaves.operation1Interleave ) );
        newParams.put( OPERATION_3_INTERLEAVE_KEY, Long.toString( interleaves.operation3Interleave ) );
        newParams.put( OPERATION_4_INTERLEAVE_KEY, Long.toString( interleaves.operation4Interleave ) );
        newParams.put( OPERATION_5_INTERLEAVE_KEY, Long.toString( interleaves.operation5Interleave ) );
        newParams.put( OPERATION_6_INTERLEAVE_KEY, Long.toString( interleaves.operation6Interleave ) );
        newParams.put( OPERATION_7_INTERLEAVE_KEY, Long.toString( interleaves.operation7Interleave ) );
        newParams.put( OPERATION_8_INTERLEAVE_KEY, Long.toString( interleaves.operation8Interleave ) );
        newParams.put( OPERATION_10_INTERLEAVE_KEY, Long.toString( interleaves.operation10Interleave ) );
        newParams.put( OPERATION_14_INTERLEAVE_KEY, Long.toString( interleaves.operation14Interleave ) );
        newParams.put( OPERATION_16_INTERLEAVE_KEY, Long.toString( interleaves.operation16Interleave ) );
        newParams.put( OPERATION_17_INTERLEAVE_KEY, Long.toString( interleaves.operation17Interleave ) );
        newParams.put( OPERATION_18_INTERLEAVE_KEY, Long.toString( interleaves.operation18Interleave ) );
        newParams.put( OPERATION_21_INTERLEAVE_KEY, Long.toString( interleaves.operation21Interleave ) );
        newParams.put( OPERATION_22_INTERLEAVE_KEY, Long.toString( interleaves.operation22Interleave ) );
        newParams.put( OPERATION_25_INTERLEAVE_KEY, Long.toString( interleaves.operation25Interleave ) );
        return newParams;
    }

    public static LdbcSnbBiInterleaves interleavesFromFrequencyParams( Map<String,String> params )
    {
        // TODO revise/replace
        Integer minimumDistanceAsMilli = 1;
        return LdbcSnbBiInterleaves.fromFrequencies(
                minimumDistanceAsMilli,
                Long.parseLong( params.get( OPERATION_1_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_3_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_4_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_5_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_6_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_7_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_8_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_10_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_14_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_16_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_17_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_18_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_21_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_22_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_25_FREQUENCY_KEY ) )
        );
    }

    public static LdbcSnbBiInterleaves interleavesFromInterleaveParams(
            Map<String,String> params )
    {
        return new LdbcSnbBiInterleaves(
                Long.parseLong( params.get( OPERATION_1_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_3_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_4_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_5_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_6_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_7_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_8_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_10_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_14_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_16_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_17_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_18_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_21_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_22_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_25_INTERLEAVE_KEY ) )
        );
    }

    static class LdbcSnbBiInterleaves
    {
        final long operation1Interleave;
        final long operation3Interleave;
        final long operation4Interleave;
        final long operation5Interleave;
        final long operation6Interleave;
        final long operation7Interleave;
        final long operation8Interleave;
        final long operation10Interleave;
        final long operation14Interleave;
        final long operation16Interleave;
        final long operation17Interleave;
        final long operation18Interleave;
        final long operation21Interleave;
        final long operation22Interleave;
        final long operation25Interleave;

        static LdbcSnbBiInterleaves fromFrequencies(
                long minimumDistanceAsMilli,
                long operation1Frequency,
                long operation3Frequency,
                long operation4Frequency,
                long operation5Frequency,
                long operation6Frequency,
                long operation7Frequency,
                long operation8Frequency,
                long operation10Frequency,
                long operation14Frequency,
                long operation16Frequency,
                long operation17Frequency,
                long operation18Frequency,
                long operation21Frequency,
                long operation22Frequency,
                long operation25Frequency )
        {
            return new LdbcSnbBiInterleaves(
                    operation1Frequency * minimumDistanceAsMilli,
                    operation3Frequency * minimumDistanceAsMilli,
                    operation4Frequency * minimumDistanceAsMilli,
                    operation5Frequency * minimumDistanceAsMilli,
                    operation6Frequency * minimumDistanceAsMilli,
                    operation7Frequency * minimumDistanceAsMilli,
                    operation8Frequency * minimumDistanceAsMilli,
                    operation10Frequency * minimumDistanceAsMilli,
                    operation14Frequency * minimumDistanceAsMilli,
                    operation16Frequency * minimumDistanceAsMilli,
                    operation17Frequency * minimumDistanceAsMilli,
                    operation18Frequency * minimumDistanceAsMilli,
                    operation21Frequency * minimumDistanceAsMilli,
                    operation22Frequency * minimumDistanceAsMilli,
                    operation25Frequency * minimumDistanceAsMilli
            );
        }

        private LdbcSnbBiInterleaves(
                long operation1Interleave,
                long operation3Interleave,
                long operation4Interleave,
                long operation5Interleave,
                long operation6Interleave,
                long operation7Interleave,
                long operation8Interleave,
                long operation10Interleave,
                long operation14Interleave,
                long operation16Interleave,
                long operation17Interleave,
                long operation18Interleave,
                long operation21Interleave,
                long operation22Interleave,
                long operation25Interleave )
        {
            this.operation1Interleave = operation1Interleave;
            this.operation3Interleave = operation3Interleave;
            this.operation4Interleave = operation4Interleave;
            this.operation5Interleave = operation5Interleave;
            this.operation6Interleave = operation6Interleave;
            this.operation7Interleave = operation7Interleave;
            this.operation8Interleave = operation8Interleave;
            this.operation10Interleave = operation10Interleave;
            this.operation14Interleave = operation14Interleave;
            this.operation16Interleave = operation16Interleave;
            this.operation17Interleave = operation17Interleave;
            this.operation18Interleave = operation18Interleave;
            this.operation21Interleave = operation21Interleave;
            this.operation22Interleave = operation22Interleave;
            this.operation25Interleave = operation25Interleave;
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
        mapping.put( LdbcSnbBiQuery3TagEvolution.TYPE, LdbcSnbBiQuery3TagEvolution.class );
        mapping.put( LdbcSnbBiQuery4PopularCountryTopics.TYPE, LdbcSnbBiQuery4PopularCountryTopics.class );
        mapping.put( LdbcSnbBiQuery5TopCountryPosters.TYPE, LdbcSnbBiQuery5TopCountryPosters.class );
        mapping.put( LdbcSnbBiQuery6ActivePosters.TYPE, LdbcSnbBiQuery6ActivePosters.class );
        mapping.put( LdbcSnbBiQuery7AuthoritativeUsers.TYPE, LdbcSnbBiQuery7AuthoritativeUsers.class );
        mapping.put( LdbcSnbBiQuery8RelatedTopics.TYPE, LdbcSnbBiQuery8RelatedTopics.class );
        mapping.put( LdbcSnbBiQuery10TagPerson.TYPE, LdbcSnbBiQuery10TagPerson.class );
        mapping.put( LdbcSnbBiQuery14TopThreadInitiators.TYPE, LdbcSnbBiQuery14TopThreadInitiators.class );
        mapping.put( LdbcSnbBiQuery16ExpertsInSocialCircle.TYPE, LdbcSnbBiQuery16ExpertsInSocialCircle.class );
        mapping.put( LdbcSnbBiQuery17FriendshipTriangles.TYPE, LdbcSnbBiQuery17FriendshipTriangles.class );
        mapping.put( LdbcSnbBiQuery18PersonPostCounts.TYPE, LdbcSnbBiQuery18PersonPostCounts.class );
        mapping.put( LdbcSnbBiQuery21Zombies.TYPE, LdbcSnbBiQuery21Zombies.class );
        mapping.put( LdbcSnbBiQuery22InternationalDialog.TYPE, LdbcSnbBiQuery22InternationalDialog.class );
        mapping.put( LdbcSnbBiQuery25WeightedPaths.TYPE, LdbcSnbBiQuery25WeightedPaths.class );
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
               params.containsKey( OPERATION_3_ENABLE_KEY ) ||
               params.containsKey( OPERATION_4_ENABLE_KEY ) ||
               params.containsKey( OPERATION_5_ENABLE_KEY ) ||
               params.containsKey( OPERATION_6_ENABLE_KEY ) ||
               params.containsKey( OPERATION_7_ENABLE_KEY ) ||
               params.containsKey( OPERATION_8_ENABLE_KEY ) ||
               params.containsKey( OPERATION_10_ENABLE_KEY ) ||
               params.containsKey( OPERATION_14_ENABLE_KEY ) ||
               params.containsKey( OPERATION_16_ENABLE_KEY ) ||
               params.containsKey( OPERATION_17_ENABLE_KEY ) ||
               params.containsKey( OPERATION_18_ENABLE_KEY ) ||
               params.containsKey( OPERATION_21_ENABLE_KEY ) ||
               params.containsKey( OPERATION_22_ENABLE_KEY ) ||
               params.containsKey( OPERATION_25_ENABLE_KEY );
    }

    public static boolean hasWrites( Map<String,String> params )
    {
        return false;
    }
}
