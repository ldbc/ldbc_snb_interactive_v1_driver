package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.DriverConfigurationFileHelper;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.ClassLoadingException;
import com.ldbc.driver.util.MapUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery2TopTags.class.getSimpleName() + FREQUENCY_SUFFIX;
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
    public final static String OPERATION_9_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery9RelatedForums.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_10_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery10TagPerson.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_11_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery11UnrelatedReplies.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_12_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery12TrendingPosts.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_13_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery13PopularMonthlyTags.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_14_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery14TopThreadInitiators.class.getSimpleName() +
            FREQUENCY_SUFFIX;
    public final static String OPERATION_15_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery15SocialNormals.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_16_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery16ExpertsInSocialCircle.class.getSimpleName() +
            FREQUENCY_SUFFIX;
    public final static String OPERATION_17_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery17FriendshipTriangles.class.getSimpleName() +
            FREQUENCY_SUFFIX;
    public final static String OPERATION_18_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery18PersonPostCounts.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_19_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery19StrangerInteraction.class.getSimpleName() +
            FREQUENCY_SUFFIX;
    public final static String OPERATION_20_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery20HighLevelTopics.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_21_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery21Zombies.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String OPERATION_22_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery22InternationalDialog.class.getSimpleName() +
            FREQUENCY_SUFFIX;
    public final static String OPERATION_23_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery23HolidayDestinations.class.getSimpleName() +
            FREQUENCY_SUFFIX;
    public final static String OPERATION_24_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery24MessagesByTopic.class.getSimpleName() + FREQUENCY_SUFFIX;
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
            OPERATION_15_FREQUENCY_KEY,
            OPERATION_16_FREQUENCY_KEY,
            OPERATION_17_FREQUENCY_KEY,
            OPERATION_18_FREQUENCY_KEY,
            OPERATION_19_FREQUENCY_KEY,
            OPERATION_20_FREQUENCY_KEY,
            OPERATION_21_FREQUENCY_KEY,
            OPERATION_22_FREQUENCY_KEY,
            OPERATION_23_FREQUENCY_KEY,
            OPERATION_24_FREQUENCY_KEY
    );

    private static Map<Integer,String> typeToFrequencyKeyMapping()
    {
        Map<Integer,String> mapping = new HashMap<>();
        mapping.put( LdbcSnbBiQuery1PostingSummary.TYPE, OPERATION_1_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery2TopTags.TYPE, OPERATION_2_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery3TagEvolution.TYPE, OPERATION_3_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery4PopularCountryTopics.TYPE, OPERATION_4_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery5TopCountryPosters.TYPE, OPERATION_5_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery6ActivePosters.TYPE, OPERATION_6_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery7AuthoritativeUsers.TYPE, OPERATION_7_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery8RelatedTopics.TYPE, OPERATION_8_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery9RelatedForums.TYPE, OPERATION_9_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery10TagPerson.TYPE, OPERATION_10_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery11UnrelatedReplies.TYPE, OPERATION_11_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery12TrendingPosts.TYPE, OPERATION_12_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery13PopularMonthlyTags.TYPE, OPERATION_13_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery14TopThreadInitiators.TYPE, OPERATION_14_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery15SocialNormals.TYPE, OPERATION_15_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery16ExpertsInSocialCircle.TYPE, OPERATION_16_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery17FriendshipTriangles.TYPE, OPERATION_17_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery18PersonPostCounts.TYPE, OPERATION_18_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery19StrangerInteraction.TYPE, OPERATION_19_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery20HighLevelTopics.TYPE, OPERATION_20_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery21Zombies.TYPE, OPERATION_21_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery22InternationalDialog.TYPE, OPERATION_22_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery23HolidayDestinations.TYPE, OPERATION_23_FREQUENCY_KEY );
        mapping.put( LdbcSnbBiQuery24MessagesByTopic.TYPE, OPERATION_24_FREQUENCY_KEY );
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
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery2TopTags.class.getSimpleName() + INTERLEAVE_SUFFIX;
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
    public final static String OPERATION_9_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery9RelatedForums.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_10_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery10TagPerson.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_11_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery11UnrelatedReplies.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_12_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery12TrendingPosts.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_13_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery13PopularMonthlyTags.class.getSimpleName() +
            INTERLEAVE_SUFFIX;
    public final static String OPERATION_14_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery14TopThreadInitiators.class.getSimpleName() +
            INTERLEAVE_SUFFIX;
    public final static String OPERATION_15_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery15SocialNormals.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_16_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery16ExpertsInSocialCircle.class.getSimpleName() +
            INTERLEAVE_SUFFIX;
    public final static String OPERATION_17_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery17FriendshipTriangles.class.getSimpleName() +
            INTERLEAVE_SUFFIX;
    public final static String OPERATION_18_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery18PersonPostCounts.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_19_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery19StrangerInteraction.class.getSimpleName() +
            INTERLEAVE_SUFFIX;
    public final static String OPERATION_20_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery20HighLevelTopics.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_21_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery21Zombies.class.getSimpleName() + INTERLEAVE_SUFFIX;
    public final static String OPERATION_22_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery22InternationalDialog.class.getSimpleName() +
            INTERLEAVE_SUFFIX;
    public final static String OPERATION_23_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery23HolidayDestinations.class.getSimpleName() +
            INTERLEAVE_SUFFIX;
    public final static String OPERATION_24_INTERLEAVE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery24MessagesByTopic.class.getSimpleName() + INTERLEAVE_SUFFIX;
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
            OPERATION_15_INTERLEAVE_KEY,
            OPERATION_16_INTERLEAVE_KEY,
            OPERATION_17_INTERLEAVE_KEY,
            OPERATION_18_INTERLEAVE_KEY,
            OPERATION_19_INTERLEAVE_KEY,
            OPERATION_20_INTERLEAVE_KEY,
            OPERATION_21_INTERLEAVE_KEY,
            OPERATION_22_INTERLEAVE_KEY,
            OPERATION_23_INTERLEAVE_KEY,
            OPERATION_24_INTERLEAVE_KEY
    );

    private static Map<Integer,String> typeToInterleaveKeyMapping()
    {
        Map<Integer,String> mapping = new HashMap<>();
        mapping.put( LdbcSnbBiQuery1PostingSummary.TYPE, OPERATION_1_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery2TopTags.TYPE, OPERATION_2_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery3TagEvolution.TYPE, OPERATION_3_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery4PopularCountryTopics.TYPE, OPERATION_4_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery5TopCountryPosters.TYPE, OPERATION_5_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery6ActivePosters.TYPE, OPERATION_6_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery7AuthoritativeUsers.TYPE, OPERATION_7_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery8RelatedTopics.TYPE, OPERATION_8_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery9RelatedForums.TYPE, OPERATION_9_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery10TagPerson.TYPE, OPERATION_10_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery11UnrelatedReplies.TYPE, OPERATION_11_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery12TrendingPosts.TYPE, OPERATION_12_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery13PopularMonthlyTags.TYPE, OPERATION_13_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery14TopThreadInitiators.TYPE, OPERATION_14_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery15SocialNormals.TYPE, OPERATION_15_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery16ExpertsInSocialCircle.TYPE, OPERATION_16_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery17FriendshipTriangles.TYPE, OPERATION_17_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery18PersonPostCounts.TYPE, OPERATION_18_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery19StrangerInteraction.TYPE, OPERATION_19_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery20HighLevelTopics.TYPE, OPERATION_20_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery21Zombies.TYPE, OPERATION_21_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery22InternationalDialog.TYPE, OPERATION_22_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery23HolidayDestinations.TYPE, OPERATION_23_INTERLEAVE_KEY );
        mapping.put( LdbcSnbBiQuery24MessagesByTopic.TYPE, OPERATION_24_INTERLEAVE_KEY );
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
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery2TopTags.class.getSimpleName() + ENABLE_SUFFIX;
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
    public final static String OPERATION_9_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery9RelatedForums.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_10_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery10TagPerson.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_11_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery11UnrelatedReplies.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_12_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery12TrendingPosts.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_13_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery13PopularMonthlyTags.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_14_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery14TopThreadInitiators.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_15_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery15SocialNormals.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_16_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery16ExpertsInSocialCircle.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_17_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery17FriendshipTriangles.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_18_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery18PersonPostCounts.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_19_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery19StrangerInteraction.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_20_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery20HighLevelTopics.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_21_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery21Zombies.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_22_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery22InternationalDialog.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_23_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery23HolidayDestinations.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String OPERATION_24_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery24MessagesByTopic.class.getSimpleName() + ENABLE_SUFFIX;
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
            OPERATION_15_ENABLE_KEY,
            OPERATION_16_ENABLE_KEY,
            OPERATION_17_ENABLE_KEY,
            OPERATION_18_ENABLE_KEY,
            OPERATION_19_ENABLE_KEY,
            OPERATION_20_ENABLE_KEY,
            OPERATION_21_ENABLE_KEY,
            OPERATION_22_ENABLE_KEY,
            OPERATION_23_ENABLE_KEY,
            OPERATION_24_ENABLE_KEY
    );

    /*
     * Read Operation Parameters
     */
    public final static String OPERATION_1_PARAMS_FILENAME = "q1_param.txt";
    public final static String OPERATION_2_PARAMS_FILENAME = "q2_param.txt";
    public final static String OPERATION_3_PARAMS_FILENAME = "q3_param.txt";
    public final static String OPERATION_4_PARAMS_FILENAME = "q4_param.txt";
    public final static String OPERATION_5_PARAMS_FILENAME = "q5_param.txt";
    public final static String OPERATION_6_PARAMS_FILENAME = "q6_param.txt";
    public final static String OPERATION_7_PARAMS_FILENAME = "q7_param.txt";
    public final static String OPERATION_8_PARAMS_FILENAME = "q8_param.txt";
    public final static String OPERATION_9_PARAMS_FILENAME = "q9_param.txt";
    public final static String OPERATION_10_PARAMS_FILENAME = "q10_param.txt";
    public final static String OPERATION_11_PARAMS_FILENAME = "q11_param.txt";
    public final static String OPERATION_12_PARAMS_FILENAME = "q12_param.txt";
    public final static String OPERATION_13_PARAMS_FILENAME = "q13_param.txt";
    public final static String OPERATION_14_PARAMS_FILENAME = "q14_param.txt";
    public final static String OPERATION_15_PARAMS_FILENAME = "q15_param.txt";
    public final static String OPERATION_16_PARAMS_FILENAME = "q16_param.txt";
    public final static String OPERATION_17_PARAMS_FILENAME = "q17_param.txt";
    public final static String OPERATION_18_PARAMS_FILENAME = "q18_param.txt";
    public final static String OPERATION_19_PARAMS_FILENAME = "q19_param.txt";
    public final static String OPERATION_20_PARAMS_FILENAME = "q20_param.txt";
    public final static String OPERATION_21_PARAMS_FILENAME = "q21_param.txt";
    public final static String OPERATION_22_PARAMS_FILENAME = "q22_param.txt";
    public final static String OPERATION_23_PARAMS_FILENAME = "q23_param.txt";
    public final static String OPERATION_24_PARAMS_FILENAME = "q24_param.txt";
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
            OPERATION_15_PARAMS_FILENAME,
            OPERATION_16_PARAMS_FILENAME,
            OPERATION_17_PARAMS_FILENAME,
            OPERATION_18_PARAMS_FILENAME,
            OPERATION_19_PARAMS_FILENAME,
            OPERATION_20_PARAMS_FILENAME,
            OPERATION_21_PARAMS_FILENAME,
            OPERATION_22_PARAMS_FILENAME,
            OPERATION_23_PARAMS_FILENAME,
            OPERATION_24_PARAMS_FILENAME
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
        newParams.put( OPERATION_16_INTERLEAVE_KEY, Long.toString( interleaves.operation16Interleave ) );
        newParams.put( OPERATION_17_INTERLEAVE_KEY, Long.toString( interleaves.operation17Interleave ) );
        newParams.put( OPERATION_18_INTERLEAVE_KEY, Long.toString( interleaves.operation18Interleave ) );
        newParams.put( OPERATION_19_INTERLEAVE_KEY, Long.toString( interleaves.operation19Interleave ) );
        newParams.put( OPERATION_20_INTERLEAVE_KEY, Long.toString( interleaves.operation20Interleave ) );
        newParams.put( OPERATION_21_INTERLEAVE_KEY, Long.toString( interleaves.operation21Interleave ) );
        newParams.put( OPERATION_22_INTERLEAVE_KEY, Long.toString( interleaves.operation22Interleave ) );
        newParams.put( OPERATION_23_INTERLEAVE_KEY, Long.toString( interleaves.operation23Interleave ) );
        newParams.put( OPERATION_24_INTERLEAVE_KEY, Long.toString( interleaves.operation24Interleave ) );
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
                Long.parseLong( params.get( OPERATION_15_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_16_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_17_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_18_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_19_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_20_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_21_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_22_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_23_FREQUENCY_KEY ) ),
                Long.parseLong( params.get( OPERATION_24_FREQUENCY_KEY ) )
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
                Long.parseLong( params.get( OPERATION_15_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_16_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_17_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_18_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_19_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_20_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_21_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_22_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_23_INTERLEAVE_KEY ) ),
                Long.parseLong( params.get( OPERATION_24_INTERLEAVE_KEY ) )
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
        final long operation16Interleave;
        final long operation17Interleave;
        final long operation18Interleave;
        final long operation19Interleave;
        final long operation20Interleave;
        final long operation21Interleave;
        final long operation22Interleave;
        final long operation23Interleave;
        final long operation24Interleave;

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
                long operation15Frequency,
                long operation16Frequency,
                long operation17Frequency,
                long operation18Frequency,
                long operation19Frequency,
                long operation20Frequency,
                long operation21Frequency,
                long operation22Frequency,
                long operation23Frequency,
                long operation24Frequency )
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
                    operation15Frequency * minimumDistanceAsMilli,
                    operation16Frequency * minimumDistanceAsMilli,
                    operation17Frequency * minimumDistanceAsMilli,
                    operation18Frequency * minimumDistanceAsMilli,
                    operation19Frequency * minimumDistanceAsMilli,
                    operation20Frequency * minimumDistanceAsMilli,
                    operation21Frequency * minimumDistanceAsMilli,
                    operation22Frequency * minimumDistanceAsMilli,
                    operation23Frequency * minimumDistanceAsMilli,
                    operation24Frequency * minimumDistanceAsMilli
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
                long operation15Interleave,
                long operation16Interleave,
                long operation17Interleave,
                long operation18Interleave,
                long operation19Interleave,
                long operation20Interleave,
                long operation21Interleave,
                long operation22Interleave,
                long operation23Interleave,
                long operation24Interleave )
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
            this.operation16Interleave = operation16Interleave;
            this.operation17Interleave = operation17Interleave;
            this.operation18Interleave = operation18Interleave;
            this.operation19Interleave = operation19Interleave;
            this.operation20Interleave = operation20Interleave;
            this.operation21Interleave = operation21Interleave;
            this.operation22Interleave = operation22Interleave;
            this.operation23Interleave = operation23Interleave;
            this.operation24Interleave = operation24Interleave;
        }
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
        return new File( workloadsDirectory, "ldbc/snb/bi/ldbc_snb_bi_SF-0001.properties" );
    }

    public static Map<String,String> defaultConfigSF1() throws DriverConfigurationException, IOException
    {
        return ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys(
                MapUtils.loadPropertiesToMap(
                        defaultConfigFileSF1()
                )
        );
    }

    public static Map<Integer,Class<? extends Operation>> operationTypeToClassMapping()
    {
        Map<Integer,Class<? extends Operation>> operationTypeToClassMapping = new HashMap<>();
        operationTypeToClassMapping.put( LdbcSnbBiQuery1PostingSummary.TYPE, LdbcSnbBiQuery1PostingSummary.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery2TopTags.TYPE, LdbcSnbBiQuery2TopTags.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery3TagEvolution.TYPE, LdbcSnbBiQuery3TagEvolution.class );
        operationTypeToClassMapping
                .put( LdbcSnbBiQuery4PopularCountryTopics.TYPE, LdbcSnbBiQuery4PopularCountryTopics.class );
        operationTypeToClassMapping
                .put( LdbcSnbBiQuery5TopCountryPosters.TYPE, LdbcSnbBiQuery5TopCountryPosters.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery6ActivePosters.TYPE, LdbcSnbBiQuery6ActivePosters.class );
        operationTypeToClassMapping
                .put( LdbcSnbBiQuery7AuthoritativeUsers.TYPE, LdbcSnbBiQuery7AuthoritativeUsers.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery8RelatedTopics.TYPE, LdbcSnbBiQuery8RelatedTopics.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery9RelatedForums.TYPE, LdbcSnbBiQuery9RelatedForums.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery10TagPerson.TYPE, LdbcSnbBiQuery10TagPerson.class );
        operationTypeToClassMapping
                .put( LdbcSnbBiQuery11UnrelatedReplies.TYPE, LdbcSnbBiQuery11UnrelatedReplies.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery12TrendingPosts.TYPE, LdbcSnbBiQuery12TrendingPosts.class );
        operationTypeToClassMapping
                .put( LdbcSnbBiQuery13PopularMonthlyTags.TYPE, LdbcSnbBiQuery13PopularMonthlyTags.class );
        operationTypeToClassMapping
                .put( LdbcSnbBiQuery14TopThreadInitiators.TYPE, LdbcSnbBiQuery14TopThreadInitiators.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery15SocialNormals.TYPE, LdbcSnbBiQuery15SocialNormals.class );
        operationTypeToClassMapping
                .put( LdbcSnbBiQuery16ExpertsInSocialCircle.TYPE, LdbcSnbBiQuery16ExpertsInSocialCircle.class );
        operationTypeToClassMapping
                .put( LdbcSnbBiQuery17FriendshipTriangles.TYPE, LdbcSnbBiQuery17FriendshipTriangles.class );
        operationTypeToClassMapping
                .put( LdbcSnbBiQuery18PersonPostCounts.TYPE, LdbcSnbBiQuery18PersonPostCounts.class );
        operationTypeToClassMapping
                .put( LdbcSnbBiQuery19StrangerInteraction.TYPE, LdbcSnbBiQuery19StrangerInteraction.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery20HighLevelTopics.TYPE, LdbcSnbBiQuery20HighLevelTopics.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery21Zombies.TYPE, LdbcSnbBiQuery21Zombies.class );
        operationTypeToClassMapping
                .put( LdbcSnbBiQuery22InternationalDialog.TYPE, LdbcSnbBiQuery22InternationalDialog.class );
        operationTypeToClassMapping
                .put( LdbcSnbBiQuery23HolidayDestinations.TYPE, LdbcSnbBiQuery23HolidayDestinations.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery24MessagesByTopic.TYPE, LdbcSnbBiQuery24MessagesByTopic.class );
        return operationTypeToClassMapping;
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
}
