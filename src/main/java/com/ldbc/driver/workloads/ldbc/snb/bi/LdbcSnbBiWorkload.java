package com.ldbc.driver.workloads.ldbc.snb.bi;

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
    public Map<Integer,Class<? extends Operation>> operationTypeToClassMapping()
    {
        return LdbcSnbBiWorkloadConfiguration.operationTypeToClassMapping();
    }

    @Override
    public void onInit( Map<String,String> params ) throws WorkloadException
    {
        List<String> compulsoryKeys = new ArrayList<>();
        compulsoryKeys.add( LdbcSnbBiWorkloadConfiguration.PARAMETERS_DIRECTORY );
        compulsoryKeys.addAll( LdbcSnbBiWorkloadConfiguration.OPERATION_ENABLE_KEYS );
        compulsoryKeys.addAll( LdbcSnbBiWorkloadConfiguration.OPERATION_FREQUENCY_KEYS );

        Set<String> missingParameters = LdbcSnbBiWorkloadConfiguration.missingParameters( params, compulsoryKeys );
        if ( false == missingParameters.isEmpty() )
        {
            throw new WorkloadException( format(
                    "%s could not initialize due to missing parameters: %s",
                    getClass().getSimpleName(),
                    missingParameters.toString()
            ) );
        }

        File parametersDir = new File( params.get( LdbcSnbBiWorkloadConfiguration.PARAMETERS_DIRECTORY ).trim() );
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
            String operationEnabledString = params.get( operationEnableKey ).trim();
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
                Double.parseDouble( params.get( ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG ).trim() );
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
        long workloadStartTimeAsMilli = 0;
        WorkloadStreams ldbcSnbInteractiveWorkloadStreams = new WorkloadStreams();
        List<Iterator<?>> asynchronousNonDependencyStreamsList = new ArrayList<>();

        /*
         * Create read operation streams, with specified interleaves
         */
        // Query 1
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery1PostingSummary.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery2TopTags.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery3TagEvolution.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery4PopularCountryTopics.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery5TopCountryPosters.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery6ActivePosters.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery7AuthoritativeUsers.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery8RelatedTopics.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery9RelatedForums.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery10TagPerson.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery11UnrelatedReplies.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery12TrendingPosts.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery13PopularMonthlyTags.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery14TopThreadInitiators.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery15SocialNormals.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery16ExpertsInSocialCircle.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery17FriendshipTriangles.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery18PersonPostCounts.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery19StrangerInteraction.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery20HighLevelTopics.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery21Zombies.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery22InternationalDialog.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery23HolidayDestinations.class ) )
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
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery24MessagesByTopic.class ) )
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
    public DbValidationParametersFilter dbValidationParametersFilter( final Integer requiredValidationParameterCount )
    {
        // TODO may need to treat different operation types differently, or insert other logic
        return new DbValidationParametersFilter()
        {
            private final List<Operation> injectedOperations = new ArrayList<>();
            int validationParameterCount = 0;

            @Override
            public boolean useOperation( Operation operation )
            {
                return true;
            }

            @Override
            public DbValidationParametersFilterResult useOperationAndResultForValidation(
                    Operation operation,
                    Object operationResult )
            {
                if ( validationParameterCount < requiredValidationParameterCount )
                {
                    validationParameterCount++;
                    return new DbValidationParametersFilterResult(
                            DbValidationParametersFilterAcceptance.ACCEPT_AND_CONTINUE,
                            injectedOperations
                    );
                }
                else
                {
                    return new DbValidationParametersFilterResult(
                            DbValidationParametersFilterAcceptance.REJECT_AND_FINISH,
                            injectedOperations
                    );
                }
            }
        };
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
            case LdbcSnbBiQuery1PostingSummary.TYPE:
            {
                LdbcSnbBiQuery1PostingSummary ldbcQuery = (LdbcSnbBiQuery1PostingSummary) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.date() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery2TopTags.TYPE:
            {
                LdbcSnbBiQuery2TopTags ldbcQuery = (LdbcSnbBiQuery2TopTags) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.dateA() );
                operationAsList.add( ldbcQuery.dateB() );
                operationAsList.add( ldbcQuery.countries() );
                operationAsList.add( ldbcQuery.messageThreshold() );
                operationAsList.add( ldbcQuery.endOfSimulationTime() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery3TagEvolution.TYPE:
            {
                LdbcSnbBiQuery3TagEvolution ldbcQuery = (LdbcSnbBiQuery3TagEvolution) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.range1Start() );
                operationAsList.add( ldbcQuery.range1End() );
                operationAsList.add( ldbcQuery.range2Start() );
                operationAsList.add( ldbcQuery.range2End() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery4PopularCountryTopics.TYPE:
            {
                LdbcSnbBiQuery4PopularCountryTopics ldbcQuery = (LdbcSnbBiQuery4PopularCountryTopics) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.tagClass() );
                operationAsList.add( ldbcQuery.country() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery5TopCountryPosters.TYPE:
            {
                LdbcSnbBiQuery5TopCountryPosters ldbcQuery = (LdbcSnbBiQuery5TopCountryPosters) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.country() );
                operationAsList.add( ldbcQuery.popularForumLimit() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery6ActivePosters.TYPE:
            {
                LdbcSnbBiQuery6ActivePosters ldbcQuery = (LdbcSnbBiQuery6ActivePosters) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.tag() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery7AuthoritativeUsers.TYPE:
            {
                LdbcSnbBiQuery7AuthoritativeUsers ldbcQuery = (LdbcSnbBiQuery7AuthoritativeUsers) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.tag() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery8RelatedTopics.TYPE:
            {
                LdbcSnbBiQuery8RelatedTopics ldbcQuery = (LdbcSnbBiQuery8RelatedTopics) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.tag() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery9RelatedForums.TYPE:
            {
                LdbcSnbBiQuery9RelatedForums ldbcQuery = (LdbcSnbBiQuery9RelatedForums) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.tagClassA() );
                operationAsList.add( ldbcQuery.tagClassB() );
                operationAsList.add( ldbcQuery.threshold() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery10TagPerson.TYPE:
            {
                LdbcSnbBiQuery10TagPerson ldbcQuery = (LdbcSnbBiQuery10TagPerson) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.tag() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery11UnrelatedReplies.TYPE:
            {
                LdbcSnbBiQuery11UnrelatedReplies ldbcQuery = (LdbcSnbBiQuery11UnrelatedReplies) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.country() );
                operationAsList.add( ldbcQuery.blackList() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery12TrendingPosts.TYPE:
            {
                LdbcSnbBiQuery12TrendingPosts ldbcQuery = (LdbcSnbBiQuery12TrendingPosts) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.date() );
                operationAsList.add( ldbcQuery.likeCount() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery13PopularMonthlyTags.TYPE:
            {
                LdbcSnbBiQuery13PopularMonthlyTags ldbcQuery = (LdbcSnbBiQuery13PopularMonthlyTags) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.country() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery14TopThreadInitiators.TYPE:
            {
                LdbcSnbBiQuery14TopThreadInitiators ldbcQuery = (LdbcSnbBiQuery14TopThreadInitiators) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.beginDate() );
                operationAsList.add( ldbcQuery.endDate() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery15SocialNormals.TYPE:
            {
                LdbcSnbBiQuery15SocialNormals ldbcQuery = (LdbcSnbBiQuery15SocialNormals) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.country() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery16ExpertsInSocialCircle.TYPE:
            {
                LdbcSnbBiQuery16ExpertsInSocialCircle ldbcQuery = (LdbcSnbBiQuery16ExpertsInSocialCircle) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.person() );
                operationAsList.add( ldbcQuery.tagClass() );
                operationAsList.add( ldbcQuery.country() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery17FriendshipTriangles.TYPE:
            {
                LdbcSnbBiQuery17FriendshipTriangles ldbcQuery = (LdbcSnbBiQuery17FriendshipTriangles) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.country() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery18PersonPostCounts.TYPE:
            {
                LdbcSnbBiQuery18PersonPostCounts ldbcQuery = (LdbcSnbBiQuery18PersonPostCounts) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.date() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery19StrangerInteraction.TYPE:
            {
                LdbcSnbBiQuery19StrangerInteraction ldbcQuery = (LdbcSnbBiQuery19StrangerInteraction) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.date() );
                operationAsList.add( ldbcQuery.tagClassA() );
                operationAsList.add( ldbcQuery.tagClassB() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery20HighLevelTopics.TYPE:
            {
                LdbcSnbBiQuery20HighLevelTopics ldbcQuery = (LdbcSnbBiQuery20HighLevelTopics) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.tagClasses() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery21Zombies.TYPE:
            {
                LdbcSnbBiQuery21Zombies ldbcQuery = (LdbcSnbBiQuery21Zombies) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.country() );
                operationAsList.add( ldbcQuery.endDate() );
                operationAsList.add( ldbcQuery.days() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery22InternationalDialog.TYPE:
            {
                LdbcSnbBiQuery22InternationalDialog ldbcQuery = (LdbcSnbBiQuery22InternationalDialog) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.countryX() );
                operationAsList.add( ldbcQuery.countryY() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery23HolidayDestinations.TYPE:
            {
                LdbcSnbBiQuery23HolidayDestinations ldbcQuery = (LdbcSnbBiQuery23HolidayDestinations) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add( ldbcQuery.getClass().getName() );
                operationAsList.add( ldbcQuery.country() );
                operationAsList.add( ldbcQuery.limit() );
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            case LdbcSnbBiQuery24MessagesByTopic.TYPE:
            {
                LdbcSnbBiQuery24MessagesByTopic ldbcQuery = (LdbcSnbBiQuery24MessagesByTopic) operation;
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

        if ( operationClassName.equals( LdbcSnbBiQuery1PostingSummary.class.getName() ) )
        {
            long date = ((Number) operationAsList.get( 1 )).longValue();
            return new LdbcSnbBiQuery1PostingSummary( date );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery2TopTags.class.getName() ) )
        {
            long dateA = ((Number) operationAsList.get( 1 )).longValue();
            long dateB = ((Number) operationAsList.get( 2 )).longValue();
            List<String> countries = (List<String>) operationAsList.get( 3 );
            int minMessageCount = ((Number) operationAsList.get( 4 )).intValue();
            long endOfSimulationTime = ((Number) operationAsList.get( 5 )).longValue();
            int limit = ((Number) operationAsList.get( 6 )).intValue();
            return new LdbcSnbBiQuery2TopTags(
                    dateA,
                    dateB,
                    countries,
                    minMessageCount,
                    endOfSimulationTime,
                    limit
            );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery3TagEvolution.class.getName() ) )
        {
            long range1Start = ((Number) operationAsList.get( 1 )).longValue();
            long range1End = ((Number) operationAsList.get( 2 )).longValue();
            long range2Start = ((Number) operationAsList.get( 3 )).longValue();
            long range2End = ((Number) operationAsList.get( 4 )).longValue();
            int limit = ((Number) operationAsList.get( 5 )).intValue();
            return new LdbcSnbBiQuery3TagEvolution( range1Start, range1End, range2Start, range2End, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery4PopularCountryTopics.class.getName() ) )
        {
            String tagClass = (String) operationAsList.get( 1 );
            String country = (String) operationAsList.get( 2 );
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcSnbBiQuery4PopularCountryTopics( tagClass, country, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery5TopCountryPosters.class.getName() ) )
        {
            String country = (String) operationAsList.get( 1 );
            int popularForumLimit = ((Number) operationAsList.get( 2 )).intValue();
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcSnbBiQuery5TopCountryPosters( country, popularForumLimit, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery6ActivePosters.class.getName() ) )
        {
            String tag = (String) operationAsList.get( 1 );
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery6ActivePosters( tag, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery7AuthoritativeUsers.class.getName() ) )
        {
            String tag = (String) operationAsList.get( 1 );
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery7AuthoritativeUsers( tag, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery8RelatedTopics.class.getName() ) )
        {
            String tag = (String) operationAsList.get( 1 );
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery8RelatedTopics( tag, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery9RelatedForums.class.getName() ) )
        {
            String tagClassA = (String) operationAsList.get( 1 );
            String tagClassB = (String) operationAsList.get( 2 );
            int threshold = ((Number) operationAsList.get( 3 )).intValue();
            int limit = ((Number) operationAsList.get( 4 )).intValue();
            return new LdbcSnbBiQuery9RelatedForums( tagClassA, tagClassB, threshold, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery10TagPerson.class.getName() ) )
        {
            String tag = (String) operationAsList.get( 1 );
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery10TagPerson( tag, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery11UnrelatedReplies.class.getName() ) )
        {
            String country = (String) operationAsList.get( 1 );
            List<String> blackList = (List<String>) operationAsList.get( 2 );
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcSnbBiQuery11UnrelatedReplies( country, blackList, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery12TrendingPosts.class.getName() ) )
        {
            long date = ((Number) operationAsList.get( 1 )).longValue();
            int likeCount = ((Number) operationAsList.get( 2 )).intValue();
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcSnbBiQuery12TrendingPosts( date, likeCount, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery13PopularMonthlyTags.class.getName() ) )
        {
            String country = (String) operationAsList.get( 1 );
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery13PopularMonthlyTags( country, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery14TopThreadInitiators.class.getName() ) )
        {
            long beginDate = ((Number) operationAsList.get( 1 )).longValue();
            long endDate = ((Number) operationAsList.get( 2 )).longValue();
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcSnbBiQuery14TopThreadInitiators( beginDate, endDate, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery15SocialNormals.class.getName() ) )
        {
            String country = (String) operationAsList.get( 1 );
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery15SocialNormals( country, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery16ExpertsInSocialCircle.class.getName() ) )
        {
            long person = ((Number) operationAsList.get( 1 )).longValue();
            String tagClass = (String) operationAsList.get( 2 );
            String country = (String) operationAsList.get( 3 );
            int limit = ((Number) operationAsList.get( 4 )).intValue();
            return new LdbcSnbBiQuery16ExpertsInSocialCircle( person, tagClass, country, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery17FriendshipTriangles.class.getName() ) )
        {
            String country = (String) operationAsList.get( 1 );
            return new LdbcSnbBiQuery17FriendshipTriangles( country );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery18PersonPostCounts.class.getName() ) )
        {
            long date = ((Number) operationAsList.get( 1 )).longValue();
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery18PersonPostCounts( date, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery19StrangerInteraction.class.getName() ) )
        {
            long date = ((Number) operationAsList.get( 1 )).longValue();
            String tagClassA = (String) operationAsList.get( 2 );
            String tagClassB = (String) operationAsList.get( 3 );
            int limit = ((Number) operationAsList.get( 4 )).intValue();
            return new LdbcSnbBiQuery19StrangerInteraction( date, tagClassA, tagClassB, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery20HighLevelTopics.class.getName() ) )
        {
            List<String> tagClasses = (List<String>) operationAsList.get( 1 );
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery20HighLevelTopics( tagClasses, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery21Zombies.class.getName() ) )
        {
            String country = (String) operationAsList.get( 1 );
            long endDate = ((Number) operationAsList.get( 2 )).longValue();
            int days = ((Number) operationAsList.get( 3 )).intValue();
            int limit = ((Number) operationAsList.get( 4 )).intValue();
            return new LdbcSnbBiQuery21Zombies( country, endDate, days, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery22InternationalDialog.class.getName() ) )
        {
            String countryA = (String) operationAsList.get( 1 );
            String countryB = (String) operationAsList.get( 2 );
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcSnbBiQuery22InternationalDialog( countryA, countryB, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery23HolidayDestinations.class.getName() ) )
        {
            String country = (String) operationAsList.get( 1 );
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery23HolidayDestinations( country, limit );
        }
        else if ( operationClassName.equals( LdbcSnbBiQuery24MessagesByTopic.class.getName() ) )
        {
            String tagClass = (String) operationAsList.get( 1 );
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcSnbBiQuery24MessagesByTopic( tagClass, limit );
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
