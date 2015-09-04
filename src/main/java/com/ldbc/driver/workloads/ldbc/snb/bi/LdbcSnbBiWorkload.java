package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.Lists;
import com.ldbc.driver.ChildOperationGenerator;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.csv.charseeker.CharSeekerParams;
import com.ldbc.driver.generator.GeneratorFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class LdbcSnbBiWorkload extends Workload
{
    // TODO these things should really all be in an instance of LdbcSnbBiWorkloadConfiguration or ...State
    // TODO alternatively they could be CloseableStream (or similar) where files and everything are in 1 class
    private List<Closeable> readOperationFileReaders = new ArrayList<>();
    private FileInputStream readOperation1FileInputStream;
    private FileInputStream readOperation2FileInputStream;
    private FileInputStream readOperation3FileInputStream;
    private FileInputStream readOperation4FileInputStream;
    private FileInputStream readOperation5FileInputStream;
    private FileInputStream readOperation6FileInputStream;
    private FileInputStream readOperation7FileInputStream;
    private FileInputStream readOperation8FileInputStream;
    private FileInputStream readOperation9FileInputStream;
    private FileInputStream readOperation10FileInputStream;
    private FileInputStream readOperation11FileInputStream;
    private FileInputStream readOperation12FileInputStream;
    private FileInputStream readOperation13FileInputStream;
    private FileInputStream readOperation14FileInputStream;
    private FileInputStream readOperation15FileInputStream;
    private FileInputStream readOperation16FileInputStream;
    private FileInputStream readOperation17FileInputStream;
    private FileInputStream readOperation18FileInputStream;
    private FileInputStream readOperation19FileInputStream;
    private FileInputStream readOperation20FileInputStream;
    private FileInputStream readOperation21FileInputStream;
    private FileInputStream readOperation22FileInputStream;
    private FileInputStream readOperation23FileInputStream;
    private FileInputStream readOperation24FileInputStream;

    // TODO these things should really all be in an instance of LdbcSnbBiWorkloadConfiguration or ...State
    private LdbcSnbBiWorkloadConfiguration.LdbcSnbBiInterleaves interleaves = null;

    private double compressionRatio;

    private Set<Class> enabledOperationTypes;

    private static final int BUFFER_SIZE = 1 * 1024 * 1024;
    private static final char COLUMN_DELIMITER = '|';
    private static final char ARRAY_DELIMITER = ';';
    private static final char TUPLE_DELIMITER = ',';
    public static final CharSeekerParams CHAR_SEEKER_PARAMS = new CharSeekerParams(
            BUFFER_SIZE,
            COLUMN_DELIMITER,
            ARRAY_DELIMITER,
            TUPLE_DELIMITER
    );

    @Override
    public Map<Integer,Class<? extends Operation>> operationTypeToClassMapping( Map<String,String> params )
    {
        return LdbcSnbBiWorkloadConfiguration.operationTypeToClassMapping();
    }

    @Override
    public void onInit( Map<String,String> params ) throws WorkloadException
    {
        List<String> compulsoryKeys = Lists.newArrayList( LdbcSnbBiWorkloadConfiguration.PARAMETERS_DIRECTORY );
        compulsoryKeys.addAll( LdbcSnbBiWorkloadConfiguration.OPERATION_ENABLE_KEYS );

        Set<String> missingParameters = LdbcSnbBiWorkloadConfiguration.missingParameters( params, compulsoryKeys );
        if ( false == missingParameters.isEmpty() )
        {
            throw new WorkloadException( format(
                    "%s could not initialize due to missing parameters: %s",
                    getClass().getSimpleName(),
                    missingParameters.toString()
            ) );
        }

        File parametersDir = new File( params.get( LdbcSnbBiWorkloadConfiguration.PARAMETERS_DIRECTORY ) );
        if ( false == parametersDir.exists() )
        {
            throw new WorkloadException( format(
                    "Parameters directory does not exist: %s",
                    parametersDir.getAbsolutePath()
            ) );
        }
        for ( String readOperationParamsFilename : LdbcSnbBiWorkloadConfiguration.OPERATION_PARAMS_FILENAMES )
        {
            if ( false == new File( parametersDir, readOperationParamsFilename ).exists() )
            {
                throw new WorkloadException( format(
                        "Read operation parameters file does not exist: %s",
                        new File( parametersDir, readOperationParamsFilename ).getAbsolutePath()
                ) );
            }
        }
        try
        {
            readOperation1FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_1_PARAMS_FILENAME )
            );
            readOperation2FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_2_PARAMS_FILENAME )
            );
            readOperation3FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_3_PARAMS_FILENAME )
            );
            readOperation4FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_4_PARAMS_FILENAME )
            );
            readOperation5FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_5_PARAMS_FILENAME )
            );
            readOperation6FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_6_PARAMS_FILENAME )
            );
            readOperation7FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_7_PARAMS_FILENAME )
            );
            readOperation8FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_8_PARAMS_FILENAME )
            );
            readOperation9FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_9_PARAMS_FILENAME )
            );
            readOperation10FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_10_PARAMS_FILENAME )
            );
            readOperation11FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_11_PARAMS_FILENAME )
            );
            readOperation12FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_12_PARAMS_FILENAME )
            );
            readOperation13FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_13_PARAMS_FILENAME )
            );
            readOperation14FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_14_PARAMS_FILENAME )
            );
            readOperation15FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_15_PARAMS_FILENAME )
            );
            readOperation16FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_16_PARAMS_FILENAME )
            );
            readOperation17FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_17_PARAMS_FILENAME )
            );
            readOperation18FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_18_PARAMS_FILENAME )
            );
            readOperation19FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_19_PARAMS_FILENAME )
            );
            readOperation20FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_20_PARAMS_FILENAME )
            );
            readOperation21FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_21_PARAMS_FILENAME )
            );
            readOperation22FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_22_PARAMS_FILENAME )
            );
            readOperation23FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_23_PARAMS_FILENAME )
            );
            readOperation24FileInputStream = new FileInputStream(
                    new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_24_PARAMS_FILENAME )
            );
        }
        catch ( FileNotFoundException e )
        {
            throw new WorkloadException( "Error load query parameters file", e );
        }

        enabledOperationTypes = new HashSet<>();
        for ( String operationEnableKey : LdbcSnbBiWorkloadConfiguration.OPERATION_ENABLE_KEYS )
        {
            String operationEnabledString = params.get( operationEnableKey );
            Boolean operationEnabled = Boolean.parseBoolean( operationEnabledString );
            Class operationClass = LdbcSnbBiWorkloadConfiguration.operationEnabledKeyToClass( operationEnableKey );
            if ( operationEnabled )
            {
                enabledOperationTypes.add( operationClass );
            }
        }

        Set<String> missingFrequencyKeys = LdbcSnbBiWorkloadConfiguration.missingParameters(
                params,
                LdbcSnbBiWorkloadConfiguration.OPERATION_FREQUENCY_KEYS
        );
        Set<String> missingInterleaveKeys = LdbcSnbBiWorkloadConfiguration.missingParameters(
                params,
                LdbcSnbBiWorkloadConfiguration.OPERATION_INTERLEAVE_KEYS
        );
        if ( missingInterleaveKeys.isEmpty() )
        {
            // do nothing, interleaves are already set
            interleaves = LdbcSnbBiWorkloadConfiguration.interleavesFromInterleaveParams( params );
            params = LdbcSnbBiWorkloadConfiguration.applyInterleaves( params, interleaves );
        }
        else if ( missingFrequencyKeys.isEmpty() )
        {
            // compute interleave based on frequencies
            interleaves = LdbcSnbBiWorkloadConfiguration.interleavesFromFrequencyParams( params );
            params = LdbcSnbBiWorkloadConfiguration.applyInterleaves( params, interleaves );
        }
        else
        {
            // if any frequencies are not set, there should be specified interleave times for read queries
            throw new WorkloadException( format(
                    "%s could not initialize. One of the following should be empty:\n" +
                    " - missing Frequency Parameters: %s\n" +
                    " - missing Interleave Parameters: %s",
                    getClass().getSimpleName(),
                    missingFrequencyKeys.toString(),
                    missingInterleaveKeys.toString()
            ) );
        }

        this.compressionRatio =
                Double.parseDouble( params.get( ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG ) );
    }

    @Override
    synchronized protected void onClose() throws IOException
    {
        for ( Closeable readOperationFileReader : readOperationFileReaders )
        {
            readOperationFileReader.close();
        }
    }

    @Override
    protected WorkloadStreams getStreams( GeneratorFactory gf, boolean hasDbConnected ) throws WorkloadException
    {
        long workloadStartTimeAsMilli = System.currentTimeMillis();
        WorkloadStreams ldbcSnbInteractiveWorkloadStreams = new WorkloadStreams();
        List<Iterator<?>> asynchronousNonDependencyStreamsList = new ArrayList<>();

        /*
         * Create read operation streams, with specified interleaves
         */
        // Query 1
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery1.class ) )
        {
            Query1EventStreamReader operation1StreamWithoutTimes = new Query1EventStreamReader(
                    readOperation1FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation1StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation1Interleave,
                                    interleaves.operation1Interleave
                            ),
                            operation1StreamWithoutTimes
                    )
            );
        }

        // Query 2
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery2.class ) )
        {
            Query2EventStreamReader operation2StreamWithoutTimes = new Query2EventStreamReader(
                    readOperation2FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation2StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation2Interleave,
                                    interleaves.operation2Interleave
                            ),
                            operation2StreamWithoutTimes
                    )
            );
        }

        // Query 3
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery3.class ) )
        {
            Query3EventStreamReader operation3StreamWithoutTimes = new Query3EventStreamReader(
                    readOperation3FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation3StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation3Interleave,
                                    interleaves.operation3Interleave
                            ),
                            operation3StreamWithoutTimes
                    )
            );
        }

        // Query 4
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery4.class ) )
        {
            Query4EventStreamReader operation4StreamWithoutTimes = null;
            operation4StreamWithoutTimes = new Query4EventStreamReader(
                    readOperation4FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation4StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation4Interleave,
                                    interleaves.operation4Interleave
                            ),
                            operation4StreamWithoutTimes
                    )
            );
        }

        // Query 5
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery5.class ) )
        {
            Query5EventStreamReader operation5StreamWithoutTimes = new Query5EventStreamReader(
                    readOperation5FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation5StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation5Interleave,
                                    interleaves.operation5Interleave
                            ),
                            operation5StreamWithoutTimes
                    )
            );
        }

        // Query 6
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery6.class ) )
        {
            Query6EventStreamReader operation6StreamWithoutTimes = new Query6EventStreamReader(
                    readOperation6FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation6StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation6Interleave,
                                    interleaves.operation6Interleave
                            ),
                            operation6StreamWithoutTimes
                    )
            );
        }

        // Query 7
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery7.class ) )
        {
            Query7EventStreamReader operation7StreamWithoutTimes = new Query7EventStreamReader(
                    readOperation7FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation7StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation7Interleave,
                                    interleaves.operation7Interleave
                            ),
                            operation7StreamWithoutTimes
                    )
            );
        }

        // Query 8
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery8.class ) )
        {
            Query8EventStreamReader operation8StreamWithoutTimes = new Query8EventStreamReader(
                    readOperation8FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation8StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation8Interleave,
                                    interleaves.operation8Interleave
                            ),
                            operation8StreamWithoutTimes
                    )
            );
        }

        // Query 9
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery9.class ) )
        {
            Query9EventStreamReader operation9StreamWithoutTimes = new Query9EventStreamReader(
                    readOperation9FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation9StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation9Interleave,
                                    interleaves.operation9Interleave
                            ),
                            operation9StreamWithoutTimes
                    )
            );
        }

        // Query 10
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery10.class ) )
        {
            Query10EventStreamReader operation10StreamWithoutTimes = new Query10EventStreamReader(
                    readOperation10FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation10StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation10Interleave,
                                    interleaves.operation10Interleave
                            ),
                            operation10StreamWithoutTimes
                    )
            );
        }

        // Query 11
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery11.class ) )
        {
            Query11EventStreamReader operation11StreamWithoutTimes = new Query11EventStreamReader(
                    readOperation11FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation11StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation11Interleave,
                                    interleaves.operation11Interleave
                            ),
                            operation11StreamWithoutTimes
                    )
            );
        }

        // Query 12
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery12.class ) )
        {
            Query12EventStreamReader operation12StreamWithoutTimes = new Query12EventStreamReader(
                    readOperation12FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation12StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation12Interleave,
                                    interleaves.operation12Interleave
                            ),
                            operation12StreamWithoutTimes
                    )
            );
        }

        // Query 13
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery13.class ) )
        {
            Query13EventStreamReader operation13StreamWithoutTimes = new Query13EventStreamReader(
                    readOperation13FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation13StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation13Interleave,
                                    interleaves.operation13Interleave
                            ),
                            operation13StreamWithoutTimes
                    )
            );
        }

        // Query 14
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery14.class ) )
        {
            Query14EventStreamReader operation14StreamWithoutTimes = new Query14EventStreamReader(
                    readOperation14FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation14StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation14Interleave,
                                    interleaves.operation14Interleave
                            ),
                            operation14StreamWithoutTimes
                    )
            );
        }

        // Query 15
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery15.class ) )
        {
            Query15EventStreamReader operation15StreamWithoutTimes = new Query15EventStreamReader(
                    readOperation15FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation15StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation15Interleave,
                                    interleaves.operation15Interleave
                            ),
                            operation15StreamWithoutTimes
                    )
            );
        }

        // Query 16
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery16.class ) )
        {
            Query16EventStreamReader operation16StreamWithoutTimes = new Query16EventStreamReader(
                    readOperation16FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation16StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation16Interleave,
                                    interleaves.operation16Interleave
                            ),
                            operation16StreamWithoutTimes
                    )
            );
        }

        // Query 17
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery17.class ) )
        {
            Query17EventStreamReader operation17StreamWithoutTimes = new Query17EventStreamReader(
                    readOperation17FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation17StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation17Interleave,
                                    interleaves.operation17Interleave
                            ),
                            operation17StreamWithoutTimes
                    )
            );
        }

        // Query 18
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery18.class ) )
        {
            Query18EventStreamReader operation18StreamWithoutTimes = new Query18EventStreamReader(
                    readOperation18FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation18StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation18Interleave,
                                    interleaves.operation18Interleave
                            ),
                            operation18StreamWithoutTimes
                    )
            );
        }

        // Query 19
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery19.class ) )
        {
            Query19EventStreamReader operation19StreamWithoutTimes = new Query19EventStreamReader(
                    readOperation19FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation19StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation19Interleave,
                                    interleaves.operation19Interleave
                            ),
                            operation19StreamWithoutTimes
                    )
            );
        }

        // Query 20
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery20.class ) )
        {
            Query20EventStreamReader operation20StreamWithoutTimes = new Query20EventStreamReader(
                    readOperation20FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation20StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation20Interleave,
                                    interleaves.operation20Interleave
                            ),
                            operation20StreamWithoutTimes
                    )
            );
        }

        // Query 21
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery21.class ) )
        {
            Query21EventStreamReader operation21StreamWithoutTimes = new Query21EventStreamReader(
                    readOperation21FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation21StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation21Interleave,
                                    interleaves.operation21Interleave
                            ),
                            operation21StreamWithoutTimes
                    )
            );
        }

        // Query 22
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery22.class ) )
        {
            Query22EventStreamReader operation22StreamWithoutTimes = new Query22EventStreamReader(
                    readOperation22FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation22StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation22Interleave,
                                    interleaves.operation22Interleave
                            ),
                            operation22StreamWithoutTimes
                    )
            );
        }

        // Query 23
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery23.class ) )
        {
            Query23EventStreamReader operation23StreamWithoutTimes = new Query23EventStreamReader(
                    readOperation23FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation23StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation23Interleave,
                                    interleaves.operation23Interleave
                            ),
                            operation23StreamWithoutTimes
                    )
            );
        }

        // Query 24
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery24.class ) )
        {
            Query24EventStreamReader operation24StreamWithoutTimes = new Query24EventStreamReader(
                    readOperation24FileInputStream,
                    CHAR_SEEKER_PARAMS,
                    gf
            );
            readOperationFileReaders.add( operation24StreamWithoutTimes );
            asynchronousNonDependencyStreamsList.add(
                    gf.assignStartTimes(
                            gf.incrementing(
                                    workloadStartTimeAsMilli + interleaves.operation24Interleave,
                                    interleaves.operation24Interleave
                            ),
                            operation24StreamWithoutTimes
                    )
            );
        }

        /* **************
         * **************
         * **************
         *  FINAL STREAMS
         * **************
         * **************
         * **************/

        // Merge all non dependency asynchronous operation streams, ordered by operation start times
        Iterator<Operation> asynchronousNonDependencyStreams = gf.mergeSortOperationsByTimeStamp(
                asynchronousNonDependencyStreamsList.toArray(
                        new Iterator[asynchronousNonDependencyStreamsList.size()]
                )
        );

        Set<Class<? extends Operation>> dependentAsynchronousOperationTypes = new HashSet<>();
        Set<Class<? extends Operation>> dependencyAsynchronousOperationTypes = new HashSet<>();
        Iterator<Operation> asynchronousDependencyStreams = Collections.emptyIterator();
        ChildOperationGenerator shortReadsChildGenerator = null;
        ldbcSnbInteractiveWorkloadStreams.setAsynchronousStream(
                dependentAsynchronousOperationTypes,
                dependencyAsynchronousOperationTypes,
                asynchronousDependencyStreams,
                asynchronousNonDependencyStreams,
                shortReadsChildGenerator
        );

        return ldbcSnbInteractiveWorkloadStreams;
    }

    @Override
    public DbValidationParametersFilter dbValidationParametersFilter( Integer requiredValidationParameterCount )
    {
        // TODO implement
        return null;
    }

    @Override
    public long maxExpectedInterleaveAsMilli()
    {
        return TimeUnit.HOURS.toMillis( 1 );
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference TYPE_REFERENCE = new TypeReference<List<Object>>()
    {
    };

    @Override
    public String serializeOperation( Operation operation ) throws SerializingMarshallingException
    {
        try
        {
            switch ( operation.type() )
            {
            case LdbcSnbBiQuery1.TYPE:
            {
                LdbcSnbBiQuery1 ldbcQuery = (LdbcSnbBiQuery1) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.date() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery2.TYPE:
            {
                LdbcSnbBiQuery2 ldbcQuery = (LdbcSnbBiQuery2) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.dateA() );
                operationAsList.add( ldbcQuery.dateB() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery3.TYPE:
            {
                LdbcSnbBiQuery3 ldbcQuery = (LdbcSnbBiQuery3) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.dateA() );
                operationAsList.add( ldbcQuery.dateB() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery4.TYPE:
            {
                LdbcSnbBiQuery4 ldbcQuery = (LdbcSnbBiQuery4) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.tagClass() );
                operationAsList.add( ldbcQuery.country() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery5.TYPE:
            {
                LdbcSnbBiQuery5 ldbcQuery = (LdbcSnbBiQuery5) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.country() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery6.TYPE:
            {
                LdbcSnbBiQuery6 ldbcQuery = (LdbcSnbBiQuery6) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.tag() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery7.TYPE:
            {
                LdbcSnbBiQuery7 ldbcQuery = (LdbcSnbBiQuery7) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.tag() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery8.TYPE:
            {
                LdbcSnbBiQuery8 ldbcQuery = (LdbcSnbBiQuery8) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.tag() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery9.TYPE:
            {
                LdbcSnbBiQuery9 ldbcQuery = (LdbcSnbBiQuery9) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.tagClassA() );
                operationAsList.add( ldbcQuery.tagClassB() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery10.TYPE:
            {
                LdbcSnbBiQuery10 ldbcQuery = (LdbcSnbBiQuery10) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.tag() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery11.TYPE:
            {
                LdbcSnbBiQuery11 ldbcQuery = (LdbcSnbBiQuery11) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.keyWord() );
                operationAsList.add( ldbcQuery.country() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery12.TYPE:
            {
                LdbcSnbBiQuery12 ldbcQuery = (LdbcSnbBiQuery12) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.date() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery13.TYPE:
            {
                LdbcSnbBiQuery13 ldbcQuery = (LdbcSnbBiQuery13) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.country() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery14.TYPE:
            {
                LdbcSnbBiQuery14 ldbcQuery = (LdbcSnbBiQuery14) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.date() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery15.TYPE:
            {
                LdbcSnbBiQuery15 ldbcQuery = (LdbcSnbBiQuery15) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.country() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery16.TYPE:
            {
                LdbcSnbBiQuery16 ldbcQuery = (LdbcSnbBiQuery16) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.tagClass() );
                operationAsList.add( ldbcQuery.country() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery17.TYPE:
            {
                LdbcSnbBiQuery17 ldbcQuery = (LdbcSnbBiQuery17) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.country() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery18.TYPE:
            {
                LdbcSnbBiQuery18 ldbcQuery = (LdbcSnbBiQuery18) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.date() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery19.TYPE:
            {
                LdbcSnbBiQuery19 ldbcQuery = (LdbcSnbBiQuery19) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.tagClassA() );
                operationAsList.add( ldbcQuery.tagClassB() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery20.TYPE:
            {
                LdbcSnbBiQuery20 ldbcQuery = (LdbcSnbBiQuery20) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery21.TYPE:
            {
                LdbcSnbBiQuery21 ldbcQuery = (LdbcSnbBiQuery21) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.country() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery22.TYPE:
            {
                LdbcSnbBiQuery22 ldbcQuery = (LdbcSnbBiQuery22) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.countryA() );
                operationAsList.add( ldbcQuery.countryB() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery23.TYPE:
            {
                LdbcSnbBiQuery23 ldbcQuery = (LdbcSnbBiQuery23) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.country() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery24.TYPE:
            {
                LdbcSnbBiQuery24 ldbcQuery = (LdbcSnbBiQuery24) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.tagClass() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            default:
            {
                throw new SerializingMarshallingException(
                        format(
                                "Workload does not know how to serialize operation\nWorkload: %s\nOperation Type: " +
                                "%s\nOperation: %s",
                                getClass().getName(),
                                operation.getClass().getName(),
                                operation ) );
            }
            }
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException( format( "Error serializing operation\n%s", operation ), e );
        }
    }

    @Override
    public Operation marshalOperation( String serializedOperation ) throws SerializingMarshallingException
    {
        List<Object> operationAsList;
        try
        {
            operationAsList = OBJECT_MAPPER.readValue( serializedOperation, TYPE_REFERENCE );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException(
                    format( "Error while parsing serialized results\n%s", serializedOperation ), e );
        }
        String operationClassName = (String) operationAsList.get( 0 );

        if ( operationClassName.equals( LdbcSnbBiQuery1.class.getName() ) )
        {
            long date = ((Number) operationAsList.get( 1 )).longValue();
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery1( date, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery2.class.getName() ) )
        {
            long dateA = ((Number) operationAsList.get( 1 )).longValue();
            long dateB = ((Number) operationAsList.get( 2 )).longValue();
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcSnbBiQuery2( dateA, dateB, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery3.class.getName() ) )
        {
            long dateA = ((Number) operationAsList.get( 1 )).longValue();
            long dateB = ((Number) operationAsList.get( 2 )).longValue();
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcSnbBiQuery3( dateA, dateB, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery4.class.getName() ) )
        {
            String tagClass = (String) operationAsList.get( 1 );
            String country = (String) operationAsList.get( 2 );
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcSnbBiQuery4( tagClass, country, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery5.class.getName() ) )
        {
            String country = (String) operationAsList.get( 1 );
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery5( country, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery6.class.getName() ) )
        {
            String tag = (String) operationAsList.get( 1 );
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery6( tag, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery7.class.getName() ) )
        {
            String tag = (String) operationAsList.get( 1 );
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery7( tag, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery8.class.getName() ) )
        {
            String tag = (String) operationAsList.get( 1 );
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery8( tag, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery9.class.getName() ) )
        {
            String tagClassA = (String) operationAsList.get( 1 );
            String tagClassB = (String) operationAsList.get( 2 );
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcSnbBiQuery9( tagClassA, tagClassB, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery10.class.getName() ) )
        {
            String tag = (String) operationAsList.get( 1 );
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery10( tag, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery11.class.getName() ) )
        {
            String keyWord = (String) operationAsList.get( 1 );
            String country = (String) operationAsList.get( 2 );
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcSnbBiQuery11( keyWord, country, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery12.class.getName() ) )
        {
            long date = ((Number) operationAsList.get( 1 )).longValue();
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery12( date, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery13.class.getName() ) )
        {
            String country = (String) operationAsList.get( 1 );
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery13( country, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery14.class.getName() ) )
        {
            long date = ((Number) operationAsList.get( 1 )).longValue();
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery14( date, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery15.class.getName() ) )
        {
            String country = (String) operationAsList.get( 1 );
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery15( country, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery16.class.getName() ) )
        {
            String tagClass = (String) operationAsList.get( 1 );
            String country = (String) operationAsList.get( 2 );
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcSnbBiQuery16( tagClass, country, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery17.class.getName() ) )
        {
            String country = (String) operationAsList.get( 1 );
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery17( country, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery18.class.getName() ) )
        {
            long date = ((Number) operationAsList.get( 1 )).longValue();
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery18( date, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery19.class.getName() ) )
        {
            String tagClassA = (String) operationAsList.get( 1 );
            String tagClassB = (String) operationAsList.get( 2 );
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcSnbBiQuery19( tagClassA, tagClassB, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery20.class.getName() ) )
        {
            int limit = ((Number) operationAsList.get( 1 )).intValue();
            return new LdbcSnbBiQuery20( limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery21.class.getName() ) )
        {
            String country = (String) operationAsList.get( 1 );
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery21( country, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery22.class.getName() ) )
        {
            String countryA = (String) operationAsList.get( 1 );
            String countryB = (String) operationAsList.get( 2 );
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcSnbBiQuery22( countryA, countryB, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery23.class.getName() ) )
        {
            String country = (String) operationAsList.get( 1 );
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery23( country, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery24.class.getName() ) )
        {
            String tagClass = (String) operationAsList.get( 1 );
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery24( tagClass, limit );
        }

        throw new SerializingMarshallingException(
                format(
                        "Workload does not know how to marshal operation\nWorkload: %s\nAssumed Operation Type: " +
                        "%s\nSerialized Operation: %s",
                        getClass().getName(),
                        operationClassName,
                        serializedOperation ) );
    }

    @Override
    public boolean resultsEqual( Operation operation, Object result1, Object result2 ) throws WorkloadException
    {
        if ( null == result1 || null == result2 )
        {
            return false;
        }
        else
        {
            // TODO possibly implement special logic for results of some operation types?
            return result1.equals( result2 );
        }
    }
}
