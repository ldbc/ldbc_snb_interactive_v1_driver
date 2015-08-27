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
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.ClassLoadingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.ldbc.driver.util.FileUtils.removePrefix;
import static com.ldbc.driver.util.FileUtils.removeSuffix;
import static java.lang.String.format;

public class LdbcSnbBiWorkload extends Workload
{
    // TODO these things should really all be in an instance of LdbcSnbBiWorkloadConfiguration or ...State
    // TODO alternatively they could be CloseableStream (or similar) where files and everything are in 1 class
    private List<Closeable> readOperationFileReaders = new ArrayList<>();
    private File readOperation1File;
    private File readOperation2File;
    private File readOperation3File;
    private File readOperation4File;
    private File readOperation5File;
    private File readOperation6File;
    private File readOperation7File;
    private File readOperation8File;
    private File readOperation9File;
    private File readOperation10File;
    private File readOperation11File;
    private File readOperation12File;
    private File readOperation13File;
    private File readOperation14File;
    private File readOperation15File;
    private File readOperation16File;
    private File readOperation17File;
    private File readOperation18File;
    private File readOperation19File;
    private File readOperation20File;
    private File readOperation21File;
    private File readOperation22File;
    private File readOperation23File;
    private File readOperation24File;

    // TODO these things should really all be in an instance of LdbcSnbBiWorkloadConfiguration or ...State
    private LdbcSnbBiWorkloadConfiguration.LdbcSnbBiInterleaves interleaves = null;

    private double compressionRatio;

    private Set<Class> enabledOperationTypes;

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
        readOperation1File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_1_PARAMS_FILENAME );
        readOperation2File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_2_PARAMS_FILENAME );
        readOperation3File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_3_PARAMS_FILENAME );
        readOperation4File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_4_PARAMS_FILENAME );
        readOperation5File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_5_PARAMS_FILENAME );
        readOperation6File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_6_PARAMS_FILENAME );
        readOperation7File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_7_PARAMS_FILENAME );
        readOperation8File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_8_PARAMS_FILENAME );
        readOperation9File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_9_PARAMS_FILENAME );
        readOperation10File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_10_PARAMS_FILENAME );
        readOperation11File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_11_PARAMS_FILENAME );
        readOperation12File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_12_PARAMS_FILENAME );
        readOperation13File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_13_PARAMS_FILENAME );
        readOperation14File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_14_PARAMS_FILENAME );
        readOperation15File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_15_PARAMS_FILENAME );
        readOperation16File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_16_PARAMS_FILENAME );
        readOperation17File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_17_PARAMS_FILENAME );
        readOperation18File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_18_PARAMS_FILENAME );
        readOperation19File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_19_PARAMS_FILENAME );
        readOperation20File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_20_PARAMS_FILENAME );
        readOperation21File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_21_PARAMS_FILENAME );
        readOperation22File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_22_PARAMS_FILENAME );
        readOperation23File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_23_PARAMS_FILENAME );
        readOperation24File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_24_PARAMS_FILENAME );

        enabledOperationTypes = new HashSet<>();
        for ( String operationEnableKey : LdbcSnbBiWorkloadConfiguration.OPERATION_ENABLE_KEYS )
        {
            String operationEnabledString = params.get( operationEnableKey );
            Boolean operationEnabled = Boolean.parseBoolean( operationEnabledString );
            String operationClassName = LdbcSnbBiWorkloadConfiguration.LDBC_SNB_BI_PACKAGE_PREFIX +
                                        removePrefix(
                                                removeSuffix(
                                                        operationEnableKey,
                                                        LdbcSnbBiWorkloadConfiguration.ENABLE_SUFFIX
                                                ),
                                                LdbcSnbBiWorkloadConfiguration.LDBC_SNB_BI_PARAM_NAME_PREFIX
                                        );
            try
            {
                Class operationClass = ClassLoaderHelper.loadClass( operationClassName );
                if ( operationEnabled )
                {
                    enabledOperationTypes.add( operationClass );
                }
            }
            catch ( ClassLoadingException e )
            {
                throw new WorkloadException(
                        format( "Error loading operation class for parameter: %s\nGuessed class name: %s",
                                operationEnableKey, operationClassName ),
                        e
                );
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
        LdbcSnbBiWorkloadConfiguration.LdbcSnbBiInterleaves interleaves;
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
                    "%s could not initialize. One of following should be empty\nFrequencies: %s\n:Interleaves %s",
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
        long workloadStartTimeAsMilli = Long.MAX_VALUE;
        WorkloadStreams ldbcSnbInteractiveWorkloadStreams = new WorkloadStreams();
        List<Iterator<?>> asynchronousNonDependencyStreamsList = new ArrayList<>();

        /*
         * Create read operation streams, with specified interleaves
         */
        int bufferSize = 1 * 1024 * 1024;
        char columnDelimiter = '|';
        char arrayDelimiter = ';';
        char tupleDelimiter = ',';
        CharSeekerParams charSeekerParams = new CharSeekerParams(
                bufferSize,
                columnDelimiter,
                arrayDelimiter,
                tupleDelimiter
        );

        // Query 1
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery1.class ) )
        {
            Query1EventStreamReader operation1StreamWithoutTimes = new Query1EventStreamReader(
                    readOperation1File,
                    charSeekerParams,
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
                    readOperation2File,
                    charSeekerParams,
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
                    readOperation3File,
                    charSeekerParams,
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
            Query4EventStreamReader operation4StreamWithoutTimes = new Query4EventStreamReader(
                    readOperation4File,
                    charSeekerParams,
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
                    readOperation5File,
                    charSeekerParams,
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
                    readOperation6File,
                    charSeekerParams,
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
                    readOperation7File,
                    charSeekerParams,
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
                    readOperation8File,
                    charSeekerParams,
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
                    readOperation9File,
                    charSeekerParams,
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
                    readOperation10File,
                    charSeekerParams,
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
                    readOperation11File,
                    charSeekerParams,
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
                    readOperation12File,
                    charSeekerParams,
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
                    readOperation13File,
                    charSeekerParams,
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
                    readOperation14File,
                    charSeekerParams,
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
                    readOperation15File,
                    charSeekerParams,
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
                    readOperation16File,
                    charSeekerParams,
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
                    readOperation17File,
                    charSeekerParams,
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
                    readOperation18File,
                    charSeekerParams,
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
                    readOperation19File,
                    charSeekerParams,
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
                    readOperation20File,
                    charSeekerParams,
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
                    readOperation21File,
                    charSeekerParams,
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
                    readOperation22File,
                    charSeekerParams,
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
                    readOperation23File,
                    charSeekerParams,
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
                    readOperation24File,
                    charSeekerParams,
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
        // TODO implement
        // TODO move serialization logic to static methods on operation classes
        return null;
    }

    @Override
    public Operation marshalOperation( String serializedOperation ) throws SerializingMarshallingException
    {
        // TODO implement
        // TODO move marshalling logic to static methods on operation classes
        return null;
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
