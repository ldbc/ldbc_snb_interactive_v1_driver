package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.ChildOperationGenerator;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.csv.charseeker.BufferedCharSeeker;
import com.ldbc.driver.csv.charseeker.CharSeeker;
import com.ldbc.driver.csv.charseeker.Extractors;
import com.ldbc.driver.csv.charseeker.Mark;
import com.ldbc.driver.csv.charseeker.Readables;
import com.ldbc.driver.generator.CsvEventStreamReaderBasicCharSeeker;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.ClassLoadingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
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
        readOperation7File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_7_PARAMS_FILENAME );
        readOperation8File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_8_PARAMS_FILENAME );
        readOperation9File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_9_PARAMS_FILENAME );
        readOperation6File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.OPERATION_6_PARAMS_FILENAME );
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
        Set<Class<? extends Operation>> dependencyAsynchronousOperationTypes = Sets.newHashSet();

        /* *******
         * *******
         * *******
         *  READS
         * *******
         * *******
         * *******/

        /*
         * Create read operation streams, with specified interleaves
         */
        int bufferSize = 1 * 1024 * 1024;
        char columnDelimiter = '|';
        char arrayDelimiter = ';';
        char tupleDelimiter = ',';

        Iterator<Operation> readOperation1Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query1EventStreamReader.Decoder();
            Extractors extractors = new Extractors( arrayDelimiter, tupleDelimiter );
            CharSeeker charSeeker;
            try
            {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader( new FileInputStream( readOperation1File ), Charsets.UTF_8 )
                        ),
                        bufferSize
                );
            }
            catch ( FileNotFoundException e )
            {
                throw new WorkloadException(
                        format( "Unable to open parameters file: %s", readOperation1File.getAbsolutePath() ),
                        e );
            }
            Mark mark = new Mark();
            // skip headers
            try
            {
                charSeeker.seek( mark, new int[]{columnDelimiter} );
                charSeeker.seek( mark, new int[]{columnDelimiter} );
            }
            catch ( IOException e )
            {
                throw new WorkloadException(
                        format(
                                "Unable to advance parameters file beyond headers: %s",
                                readOperation1File.getAbsolutePath()
                        ),
                        e
                );
            }

            Iterator<Operation> operation1StreamWithoutTimes = new Query1EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation1StartTimes =
                    gf.incrementing(
                            workloadStartTimeAsMilli + interleaves.operation1Interleave(),
                            interleaves.operation1Interleave()
                    );

            readOperation1Stream = gf.assignStartTimes(
                    operation1StartTimes,
                    operation1StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation2Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query2EventStreamReader.Decoder();
            Extractors extractors = new Extractors( arrayDelimiter, tupleDelimiter );
            CharSeeker charSeeker;
            try
            {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader( new FileInputStream( readOperation2File ), Charsets.UTF_8 )
                        ),
                        bufferSize
                );
            }
            catch ( FileNotFoundException e )
            {
                throw new WorkloadException(
                        format( "Unable to open parameters file: %s", readOperation2File.getAbsolutePath() ),
                        e );
            }
            Mark mark = new Mark();
            // skip headers
            try
            {
                charSeeker.seek( mark, new int[]{columnDelimiter} );
                charSeeker.seek( mark, new int[]{columnDelimiter} );
            }
            catch ( IOException e )
            {
                throw new WorkloadException( format( "Unable to advance parameters file beyond headers: %s",
                        readOperation2File.getAbsolutePath() ), e );
            }

            Iterator<Operation> operation2StreamWithoutTimes = new Query2EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation2StartTimes =
                    gf.incrementing( workloadStartTimeAsMilli + interleaves.operation2Interleave(),
                            interleaves.operation2Interleave() );

            readOperation2Stream = gf.assignStartTimes(
                    operation2StartTimes,
                    operation2StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation3Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query3EventStreamReader.Decoder();
            Extractors extractors = new Extractors( arrayDelimiter, tupleDelimiter );
            CharSeeker charSeeker;
            try
            {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader( new FileInputStream( readOperation3File ), Charsets.UTF_8 )
                        ),
                        bufferSize
                );
            }
            catch ( FileNotFoundException e )
            {
                throw new WorkloadException(
                        format( "Unable to open parameters file: %s", readOperation3File.getAbsolutePath() ),
                        e );
            }
            Mark mark = new Mark();
            // skip headers
            try
            {
                charSeeker.seek( mark, new int[]{columnDelimiter} );
                charSeeker.seek( mark, new int[]{columnDelimiter} );
                charSeeker.seek( mark, new int[]{columnDelimiter} );
                charSeeker.seek( mark, new int[]{columnDelimiter} );
                charSeeker.seek( mark, new int[]{columnDelimiter} );
            }
            catch ( IOException e )
            {
                throw new WorkloadException( format( "Unable to advance parameters file beyond headers: %s",
                        readOperation3File.getAbsolutePath() ), e );
            }

            Iterator<Operation> operation3StreamWithoutTimes = new Query3EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation3StartTimes =
                    gf.incrementing( workloadStartTimeAsMilli + interleaves.operation3Interleave(),
                            interleaves.operation3Interleave() );

            readOperation3Stream = gf.assignStartTimes(
                    operation3StartTimes,
                    operation3StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation4Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query4EventStreamReader.Decoder();
            Extractors extractors = new Extractors( arrayDelimiter, tupleDelimiter );
            CharSeeker charSeeker;
            try
            {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader( new FileInputStream( readOperation4File ), Charsets.UTF_8 )
                        ),
                        bufferSize
                );
            }
            catch ( FileNotFoundException e )
            {
                throw new WorkloadException(
                        format( "Unable to open parameters file: %s", readOperation4File.getAbsolutePath() ),
                        e );
            }
            Mark mark = new Mark();
            // skip headers
            try
            {
                charSeeker.seek( mark, new int[]{columnDelimiter} );
                charSeeker.seek( mark, new int[]{columnDelimiter} );
                charSeeker.seek( mark, new int[]{columnDelimiter} );
            }
            catch ( IOException e )
            {
                throw new WorkloadException( format( "Unable to advance parameters file beyond headers: %s",
                        readOperation4File.getAbsolutePath() ), e );
            }

            Iterator<Operation> operation4StreamWithoutTimes = new Query4EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation4StartTimes =
                    gf.incrementing( workloadStartTimeAsMilli + operation4InterleaveAsMilli,
                            operation4InterleaveAsMilli );

            readOperation4Stream = gf.assignStartTimes(
                    operation4StartTimes,
                    operation4StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation5Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query5EventStreamReader.Decoder();
            Extractors extractors = new Extractors( arrayDelimiter, tupleDelimiter );
            CharSeeker charSeeker;
            try
            {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader( new FileInputStream( readOperation5File ), Charsets.UTF_8 )
                        ),
                        bufferSize
                );
            }
            catch ( FileNotFoundException e )
            {
                throw new WorkloadException(
                        format( "Unable to open parameters file: %s", readOperation5File.getAbsolutePath() ),
                        e );
            }
            Mark mark = new Mark();
            // skip headers
            try
            {
                charSeeker.seek( mark, new int[]{columnDelimiter} );
                charSeeker.seek( mark, new int[]{columnDelimiter} );
            }
            catch ( IOException e )
            {
                throw new WorkloadException( format( "Unable to advance parameters file beyond headers: %s",
                        readOperation5File.getAbsolutePath() ), e );
            }

            Iterator<Operation> operation5StreamWithoutTimes = new Query5EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation5StartTimes =
                    gf.incrementing( workloadStartTimeAsMilli + operation5InterleaveAsMilli,
                            operation5InterleaveAsMilli );

            readOperation5Stream = gf.assignStartTimes(
                    operation5StartTimes,
                    operation5StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation6Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query6EventStreamReader.Decoder();
            Extractors extractors = new Extractors( arrayDelimiter, tupleDelimiter );
            CharSeeker charSeeker;
            try
            {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader( new FileInputStream( readOperation6File ), Charsets.UTF_8 )
                        ),
                        bufferSize
                );
            }
            catch ( FileNotFoundException e )
            {
                throw new WorkloadException(
                        format( "Unable to open parameters file: %s", readOperation6File.getAbsolutePath() ),
                        e );
            }
            Mark mark = new Mark();
            // skip headers
            try
            {
                charSeeker.seek( mark, new int[]{columnDelimiter} );
                charSeeker.seek( mark, new int[]{columnDelimiter} );
            }
            catch ( IOException e )
            {
                throw new WorkloadException( format( "Unable to advance parameters file beyond headers: %s",
                        readOperation6File.getAbsolutePath() ), e );
            }

            Iterator<Operation> operation6StreamWithoutTimes = new Query6EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation6StartTimes =
                    gf.incrementing( workloadStartTimeAsMilli + operation6InterleaveAsMilli,
                            operation6InterleaveAsMilli );

            readOperation6Stream = gf.assignStartTimes(
                    operation6StartTimes,
                    operation6StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation7Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query7EventStreamReader.Decoder();
            Extractors extractors = new Extractors( arrayDelimiter, tupleDelimiter );
            CharSeeker charSeeker;
            try
            {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader( new FileInputStream( readOperation7File ), Charsets.UTF_8 )
                        ),
                        bufferSize
                );
            }
            catch ( FileNotFoundException e )
            {
                throw new WorkloadException(
                        format( "Unable to open parameters file: %s", readOperation7File.getAbsolutePath() ),
                        e );
            }
            Mark mark = new Mark();
            // skip headers
            try
            {
                charSeeker.seek( mark, new int[]{columnDelimiter} );
            }
            catch ( IOException e )
            {
                throw new WorkloadException( format( "Unable to advance parameters file beyond headers: %s",
                        readOperation7File.getAbsolutePath() ), e );
            }

            Iterator<Operation> operation7StreamWithoutTimes = new Query7EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation7StartTimes =
                    gf.incrementing( workloadStartTimeAsMilli + operation7InterleaveAsMilli,
                            operation7InterleaveAsMilli );

            readOperation7Stream = gf.assignStartTimes(
                    operation7StartTimes,
                    operation7StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation8Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query8EventStreamReader.Decoder();
            Extractors extractors = new Extractors( arrayDelimiter, tupleDelimiter );
            CharSeeker charSeeker;
            try
            {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader( new FileInputStream( readOperation8File ), Charsets.UTF_8 )
                        ),
                        bufferSize
                );
            }
            catch ( FileNotFoundException e )
            {
                throw new WorkloadException(
                        format( "Unable to open parameters file: %s", readOperation8File.getAbsolutePath() ),
                        e );
            }
            Mark mark = new Mark();
            // skip headers
            try
            {
                charSeeker.seek( mark, new int[]{columnDelimiter} );
            }
            catch ( IOException e )
            {
                throw new WorkloadException( format( "Unable to advance parameters file beyond headers: %s",
                        readOperation8File.getAbsolutePath() ), e );
            }

            Iterator<Operation> operation8StreamWithoutTimes = new Query8EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation8StartTimes =
                    gf.incrementing( workloadStartTimeAsMilli + operation8InterleaveAsMilli,
                            operation8InterleaveAsMilli );

            readOperation8Stream = gf.assignStartTimes(
                    operation8StartTimes,
                    operation8StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation9Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query9EventStreamReader.Decoder();
            Extractors extractors = new Extractors( arrayDelimiter, tupleDelimiter );
            CharSeeker charSeeker;
            try
            {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader( new FileInputStream( readOperation9File ), Charsets.UTF_8 )
                        ),
                        bufferSize
                );
            }
            catch ( FileNotFoundException e )
            {
                throw new WorkloadException(
                        format( "Unable to open parameters file: %s", readOperation9File.getAbsolutePath() ),
                        e );
            }
            Mark mark = new Mark();
            // skip headers
            try
            {
                charSeeker.seek( mark, new int[]{columnDelimiter} );
                charSeeker.seek( mark, new int[]{columnDelimiter} );
            }
            catch ( IOException e )
            {
                throw new WorkloadException( format( "Unable to advance parameters file beyond headers: %s",
                        readOperation9File.getAbsolutePath() ), e );
            }

            Iterator<Operation> operation9StreamWithoutTimes = new Query9EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation9StartTimes =
                    gf.incrementing( workloadStartTimeAsMilli + operation9InterleaveAsMilli,
                            operation9InterleaveAsMilli );

            readOperation9Stream = gf.assignStartTimes(
                    operation9StartTimes,
                    operation9StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation10Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query10EventStreamReader.Decoder();
            Extractors extractors = new Extractors( arrayDelimiter, tupleDelimiter );
            CharSeeker charSeeker;
            try
            {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader( new FileInputStream( readOperation10File ), Charsets.UTF_8 )
                        ),
                        bufferSize
                );
            }
            catch ( FileNotFoundException e )
            {
                throw new WorkloadException(
                        format( "Unable to open parameters file: %s", readOperation10File.getAbsolutePath() ),
                        e );
            }
            Mark mark = new Mark();
            // skip headers
            try
            {
                charSeeker.seek( mark, new int[]{columnDelimiter} );
                charSeeker.seek( mark, new int[]{columnDelimiter} );
            }
            catch ( IOException e )
            {
                throw new WorkloadException( format( "Unable to advance parameters file beyond headers: %s",
                        readOperation10File.getAbsolutePath() ), e );
            }

            Iterator<Operation> operation10StreamWithoutTimes = new Query10EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation10StartTimes =
                    gf.incrementing( workloadStartTimeAsMilli + operation10InterleaveAsMilli,
                            operation10InterleaveAsMilli );

            readOperation10Stream = gf.assignStartTimes(
                    operation10StartTimes,
                    operation10StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation11Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query11EventStreamReader.Decoder();
            Extractors extractors = new Extractors( arrayDelimiter, tupleDelimiter );
            CharSeeker charSeeker;
            try
            {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader( new FileInputStream( readOperation11File ), Charsets.UTF_8 )
                        ),
                        bufferSize
                );
            }
            catch ( FileNotFoundException e )
            {
                throw new WorkloadException(
                        format( "Unable to open parameters file: %s", readOperation11File.getAbsolutePath() ),
                        e );
            }
            Mark mark = new Mark();
            // skip headers
            try
            {
                charSeeker.seek( mark, new int[]{columnDelimiter} );
                charSeeker.seek( mark, new int[]{columnDelimiter} );
                charSeeker.seek( mark, new int[]{columnDelimiter} );
            }
            catch ( IOException e )
            {
                throw new WorkloadException( format( "Unable to advance parameters file beyond headers: %s",
                        readOperation11File.getAbsolutePath() ), e );
            }

            Iterator<Operation> operation11StreamWithoutTimes = new Query11EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation11StartTimes =
                    gf.incrementing( workloadStartTimeAsMilli + operation11InterleaveAsMilli,
                            operation11InterleaveAsMilli );

            readOperation11Stream = gf.assignStartTimes(
                    operation11StartTimes,
                    operation11StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation12Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query12EventStreamReader.Decoder();
            Extractors extractors = new Extractors( arrayDelimiter, tupleDelimiter );
            CharSeeker charSeeker;
            try
            {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader( new FileInputStream( readOperation12File ), Charsets.UTF_8 )
                        ),
                        bufferSize
                );
            }
            catch ( FileNotFoundException e )
            {
                throw new WorkloadException(
                        format( "Unable to open parameters file: %s", readOperation12File.getAbsolutePath() ),
                        e );
            }
            Mark mark = new Mark();
            // skip headers
            try
            {
                charSeeker.seek( mark, new int[]{columnDelimiter} );
                charSeeker.seek( mark, new int[]{columnDelimiter} );
            }
            catch ( IOException e )
            {
                throw new WorkloadException( format( "Unable to advance parameters file beyond headers: %s",
                        readOperation12File.getAbsolutePath() ), e );
            }

            Iterator<Operation> operation12StreamWithoutTimes = new Query12EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation12StartTimes =
                    gf.incrementing( workloadStartTimeAsMilli + operation12InterleaveAsMilli,
                            operation12InterleaveAsMilli );

            readOperation12Stream = gf.assignStartTimes(
                    operation12StartTimes,
                    operation12StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation13Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query13EventStreamReader.Decoder();
            Extractors extractors = new Extractors( arrayDelimiter, tupleDelimiter );
            CharSeeker charSeeker;
            try
            {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader( new FileInputStream( readOperation13File ), Charsets.UTF_8 )
                        ),
                        bufferSize
                );
            }
            catch ( FileNotFoundException e )
            {
                throw new WorkloadException(
                        format( "Unable to open parameters file: %s", readOperation13File.getAbsolutePath() ),
                        e );
            }
            Mark mark = new Mark();
            // skip headers
            try
            {
                charSeeker.seek( mark, new int[]{columnDelimiter} );
                charSeeker.seek( mark, new int[]{columnDelimiter} );
            }
            catch ( IOException e )
            {
                throw new WorkloadException( format( "Unable to advance parameters file beyond headers: %s",
                        readOperation13File.getAbsolutePath() ), e );
            }

            Iterator<Operation> operation13StreamWithoutTimes = new Query13EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation13StartTimes =
                    gf.incrementing( workloadStartTimeAsMilli + operation13InterleaveAsMilli,
                            operation13InterleaveAsMilli );

            readOperation13Stream = gf.assignStartTimes(
                    operation13StartTimes,
                    operation13StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation14Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query14EventStreamReader.Decoder();
            Extractors extractors = new Extractors( arrayDelimiter, tupleDelimiter );
            CharSeeker charSeeker;
            try
            {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader( new FileInputStream( readOperation14File ), Charsets.UTF_8 )
                        ),
                        bufferSize
                );
            }
            catch ( FileNotFoundException e )
            {
                throw new WorkloadException(
                        format( "Unable to open parameters file: %s", readOperation14File.getAbsolutePath() ),
                        e );
            }
            Mark mark = new Mark();
            // skip headers
            try
            {
                charSeeker.seek( mark, new int[]{columnDelimiter} );
                charSeeker.seek( mark, new int[]{columnDelimiter} );
            }
            catch ( IOException e )
            {
                throw new WorkloadException( format( "Unable to advance parameters file beyond headers: %s",
                        readOperation14File.getAbsolutePath() ), e );
            }

            Iterator<Operation> operation14StreamWithoutTimes = new Query14EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation14StartTimes =
                    gf.incrementing( workloadStartTimeAsMilli + operation14InterleaveAsMilli,
                            operation14InterleaveAsMilli );

            readOperation14Stream = gf.assignStartTimes(
                    operation14StartTimes,
                    operation14StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        // TODO add all queries

        if ( enabledOperationTypes.contains( LdbcSnbBiQuery1.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation1Stream );
        }
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery2.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation2Stream );
        }
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery3.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation3Stream );
        }
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery4.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation4Stream );
        }
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery5.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation5Stream );
        }
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery6.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation6Stream );
        }
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery7.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation7Stream );
        }
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery8.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation8Stream );
        }
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery9.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation9Stream );
        }
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery10.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation10Stream );
        }
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery11.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation11Stream );
        }
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery12.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation12Stream );
        }
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery13.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation13Stream );
        }
        if ( enabledOperationTypes.contains( LdbcSnbBiQuery14.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation14Stream );
        }
        // TODO add all queries

        /*
         * Merge all non dependency asynchronous operation streams, ordered by operation start times
         */
        Iterator<Operation> asynchronousNonDependencyStreams = gf.mergeSortOperationsByTimeStamp(
                asynchronousNonDependencyStreamsList
                        .toArray( new Iterator[asynchronousNonDependencyStreamsList.size()] )
        );

        /* **************
         * **************
         * **************
         *  FINAL STREAMS
         * **************
         * **************
         * **************/

        Set<Class<? extends Operation>> dependentAsynchronousOperationTypes = new HashSet<>();
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
