package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.DriverConfigurationFileHelper;
import com.ldbc.driver.util.MapUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LdbcSnbBiWorkloadConfiguration
{
    public final static String LDBC_SNB_BI_PARAM_NAME_PREFIX = "ldbc.snb.bi.";
    // directory that contains the substitution parameters files
    public final static String PARAMETERS_DIRECTORY = LDBC_SNB_BI_PARAM_NAME_PREFIX + "parameters_dir";
    public final static String LDBC_INTERACTIVE_PACKAGE_PREFIX =
            removeSuffix( LdbcSnbBiQuery1.class.getName(), LdbcSnbBiQuery1.class.getSimpleName() );

    /*
     * Operation frequency
     */
    public final static String FREQUENCY_SUFFIX = "_freq";
    public final static String READ_OPERATION_1_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery1.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_2_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery2.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_3_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery3.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_4_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery4.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_5_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery5.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_6_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery6.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_7_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery7.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_8_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery8.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_9_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery9.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_10_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery10.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_11_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery11.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_12_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery12.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_13_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery13.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_14_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery14.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_15_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery15.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_16_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery16.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_17_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery17.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_18_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery18.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_19_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery19.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_20_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery20.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_21_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery21.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_22_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery22.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_23_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery23.class.getSimpleName() + FREQUENCY_SUFFIX;
    public final static String READ_OPERATION_24_FREQUENCY_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery24.class.getSimpleName() + FREQUENCY_SUFFIX;
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
            READ_OPERATION_14_FREQUENCY_KEY,
            READ_OPERATION_15_FREQUENCY_KEY,
            READ_OPERATION_16_FREQUENCY_KEY,
            READ_OPERATION_17_FREQUENCY_KEY,
            READ_OPERATION_18_FREQUENCY_KEY,
            READ_OPERATION_19_FREQUENCY_KEY,
            READ_OPERATION_20_FREQUENCY_KEY,
            READ_OPERATION_21_FREQUENCY_KEY,
            READ_OPERATION_22_FREQUENCY_KEY,
            READ_OPERATION_23_FREQUENCY_KEY,
            READ_OPERATION_24_FREQUENCY_KEY
    );

    /*
     * Operation Enable
     */
    public final static String ENABLE_SUFFIX = "_enable";
    public final static String LONG_READ_OPERATION_1_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery1.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_2_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery2.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_3_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery3.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_4_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery4.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_5_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery5.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_6_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery6.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_7_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery7.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_8_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery8.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_9_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery9.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_10_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery10.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_11_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery11.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_12_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery12.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_13_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery13.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_14_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery14.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_15_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery15.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_16_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery16.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_17_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery17.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_18_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery18.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_19_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery19.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_20_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery20.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_21_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery21.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_22_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery22.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_23_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery23.class.getSimpleName() + ENABLE_SUFFIX;
    public final static String LONG_READ_OPERATION_24_ENABLE_KEY =
            LDBC_SNB_BI_PARAM_NAME_PREFIX + LdbcSnbBiQuery24.class.getSimpleName() + ENABLE_SUFFIX;
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
            LONG_READ_OPERATION_14_ENABLE_KEY,
            LONG_READ_OPERATION_15_ENABLE_KEY,
            LONG_READ_OPERATION_16_ENABLE_KEY,
            LONG_READ_OPERATION_17_ENABLE_KEY,
            LONG_READ_OPERATION_18_ENABLE_KEY,
            LONG_READ_OPERATION_19_ENABLE_KEY,
            LONG_READ_OPERATION_20_ENABLE_KEY,
            LONG_READ_OPERATION_21_ENABLE_KEY,
            LONG_READ_OPERATION_22_ENABLE_KEY,
            LONG_READ_OPERATION_23_ENABLE_KEY,
            LONG_READ_OPERATION_24_ENABLE_KEY
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
    public final static String READ_OPERATION_15_PARAMS_FILENAME = "query_15_param.txt";
    public final static String READ_OPERATION_16_PARAMS_FILENAME = "query_16_param.txt";
    public final static String READ_OPERATION_17_PARAMS_FILENAME = "query_17_param.txt";
    public final static String READ_OPERATION_18_PARAMS_FILENAME = "query_18_param.txt";
    public final static String READ_OPERATION_19_PARAMS_FILENAME = "query_19_param.txt";
    public final static String READ_OPERATION_20_PARAMS_FILENAME = "query_20_param.txt";
    public final static String READ_OPERATION_21_PARAMS_FILENAME = "query_21_param.txt";
    public final static String READ_OPERATION_22_PARAMS_FILENAME = "query_22_param.txt";
    public final static String READ_OPERATION_23_PARAMS_FILENAME = "query_23_param.txt";
    public final static String READ_OPERATION_24_PARAMS_FILENAME = "query_24_param.txt";
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
            READ_OPERATION_14_PARAMS_FILENAME,
            READ_OPERATION_15_PARAMS_FILENAME,
            READ_OPERATION_16_PARAMS_FILENAME,
            READ_OPERATION_17_PARAMS_FILENAME,
            READ_OPERATION_18_PARAMS_FILENAME,
            READ_OPERATION_19_PARAMS_FILENAME,
            READ_OPERATION_20_PARAMS_FILENAME,
            READ_OPERATION_21_PARAMS_FILENAME,
            READ_OPERATION_22_PARAMS_FILENAME,
            READ_OPERATION_23_PARAMS_FILENAME,
            READ_OPERATION_24_PARAMS_FILENAME
    );

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

    public static Map<Integer,Class<? extends Operation>> operationTypeToClassMapping()
    {
        Map<Integer,Class<? extends Operation>> operationTypeToClassMapping = new HashMap<>();
        operationTypeToClassMapping.put( LdbcSnbBiQuery1.TYPE, LdbcSnbBiQuery1.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery2.TYPE, LdbcSnbBiQuery2.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery3.TYPE, LdbcSnbBiQuery3.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery4.TYPE, LdbcSnbBiQuery4.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery5.TYPE, LdbcSnbBiQuery5.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery6.TYPE, LdbcSnbBiQuery6.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery7.TYPE, LdbcSnbBiQuery7.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery8.TYPE, LdbcSnbBiQuery8.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery9.TYPE, LdbcSnbBiQuery9.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery10.TYPE, LdbcSnbBiQuery10.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery11.TYPE, LdbcSnbBiQuery11.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery12.TYPE, LdbcSnbBiQuery12.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery13.TYPE, LdbcSnbBiQuery13.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery14.TYPE, LdbcSnbBiQuery14.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery15.TYPE, LdbcSnbBiQuery15.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery16.TYPE, LdbcSnbBiQuery16.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery17.TYPE, LdbcSnbBiQuery17.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery18.TYPE, LdbcSnbBiQuery18.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery19.TYPE, LdbcSnbBiQuery19.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery20.TYPE, LdbcSnbBiQuery20.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery21.TYPE, LdbcSnbBiQuery21.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery22.TYPE, LdbcSnbBiQuery22.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery23.TYPE, LdbcSnbBiQuery23.class );
        operationTypeToClassMapping.put( LdbcSnbBiQuery24.TYPE, LdbcSnbBiQuery24.class );
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
}
