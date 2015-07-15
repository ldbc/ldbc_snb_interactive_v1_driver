package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Equator;
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
import java.util.Date;
import java.util.HashMap;
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
    private long readOperation1InterleaveAsMilli;
    private long readOperation2InterleaveAsMilli;
    private long readOperation3InterleaveAsMilli;
    private long readOperation4InterleaveAsMilli;
    private long readOperation5InterleaveAsMilli;
    private long readOperation6InterleaveAsMilli;
    private long readOperation7InterleaveAsMilli;
    private long readOperation8InterleaveAsMilli;
    private long readOperation9InterleaveAsMilli;
    private long readOperation10InterleaveAsMilli;
    private long readOperation11InterleaveAsMilli;
    private long readOperation12InterleaveAsMilli;
    private long readOperation13InterleaveAsMilli;
    private long readOperation14InterleaveAsMilli;
    private long readOperation15InterleaveAsMilli;
    private long readOperation16InterleaveAsMilli;
    private long readOperation17InterleaveAsMilli;
    private long readOperation18InterleaveAsMilli;
    private long readOperation19InterleaveAsMilli;
    private long readOperation20InterleaveAsMilli;
    private long readOperation21InterleaveAsMilli;
    private long readOperation22InterleaveAsMilli;
    private long readOperation23InterleaveAsMilli;
    private long readOperation24InterleaveAsMilli;

    private double compressionRatio;

    private Set<Class> enabledReadOperationTypes;

    @Override
    public Map<Integer,Class<? extends Operation>> operationTypeToClassMapping( Map<String,String> params )
    {
        return LdbcSnbBiWorkloadConfiguration.operationTypeToClassMapping();
    }

    @Override
    public void onInit( Map<String,String> params ) throws WorkloadException
    {
        List<String> compulsoryKeys = Lists.newArrayList(
                LdbcSnbBiWorkloadConfiguration.PARAMETERS_DIRECTORY );

        compulsoryKeys.addAll( LdbcSnbBiWorkloadConfiguration.LONG_READ_OPERATION_ENABLE_KEYS );

        Set<String> missingPropertyParameters = LdbcSnbBiWorkloadConfiguration
                .missingParameters( params, compulsoryKeys );
        if ( false == missingPropertyParameters.isEmpty() )
        {
            throw new WorkloadException( format( "Workload could not initialize due to missing parameters: %s",
                    missingPropertyParameters.toString() ) );
        }


        File parametersDir = new File( params.get( LdbcSnbBiWorkloadConfiguration.PARAMETERS_DIRECTORY ) );
        if ( false == parametersDir.exists() )
        {
            throw new WorkloadException(
                    format( "Parameters directory does not exist: %s", parametersDir.getAbsolutePath() ) );
        }
        for ( String readOperationParamsFilename : LdbcSnbBiWorkloadConfiguration.READ_OPERATION_PARAMS_FILENAMES )
        {
            String readOperationParamsFullPath = parametersDir.getAbsolutePath() + "/" + readOperationParamsFilename;
            if ( false == new File( readOperationParamsFullPath ).exists() )
            {
                throw new WorkloadException( format( "Read operation parameters file does not exist: %s",
                        readOperationParamsFullPath ) );
            }
        }
        readOperation1File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.READ_OPERATION_1_PARAMS_FILENAME );
        readOperation2File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.READ_OPERATION_2_PARAMS_FILENAME );
        readOperation3File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.READ_OPERATION_3_PARAMS_FILENAME );
        readOperation4File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.READ_OPERATION_4_PARAMS_FILENAME );
        readOperation5File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.READ_OPERATION_5_PARAMS_FILENAME );
        readOperation7File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.READ_OPERATION_7_PARAMS_FILENAME );
        readOperation8File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.READ_OPERATION_8_PARAMS_FILENAME );
        readOperation9File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.READ_OPERATION_9_PARAMS_FILENAME );
        readOperation6File = new File( parametersDir, LdbcSnbBiWorkloadConfiguration.READ_OPERATION_6_PARAMS_FILENAME );
        readOperation10File =
                new File( parametersDir, LdbcSnbBiWorkloadConfiguration.READ_OPERATION_10_PARAMS_FILENAME );
        readOperation11File =
                new File( parametersDir, LdbcSnbBiWorkloadConfiguration.READ_OPERATION_11_PARAMS_FILENAME );
        readOperation12File =
                new File( parametersDir, LdbcSnbBiWorkloadConfiguration.READ_OPERATION_12_PARAMS_FILENAME );
        readOperation13File =
                new File( parametersDir, LdbcSnbBiWorkloadConfiguration.READ_OPERATION_13_PARAMS_FILENAME );
        readOperation14File =
                new File( parametersDir, LdbcSnbBiWorkloadConfiguration.READ_OPERATION_14_PARAMS_FILENAME );

        enabledReadOperationTypes = new HashSet<>();
        for ( String longReadOperationEnableKey : LdbcSnbBiWorkloadConfiguration.LONG_READ_OPERATION_ENABLE_KEYS )
        {
            String longReadOperationEnabledString = params.get( longReadOperationEnableKey );
            Boolean longReadOperationEnabled = Boolean.parseBoolean( longReadOperationEnabledString );
            String longReadOperationClassName = LdbcSnbBiWorkloadConfiguration.LDBC_SNB_BI_PACKAGE_PREFIX +
                                                removePrefix(
                                                        removeSuffix(
                                                                longReadOperationEnableKey,
                                                                LdbcSnbBiWorkloadConfiguration.ENABLE_SUFFIX
                                                        ),
                                                        LdbcSnbBiWorkloadConfiguration.LDBC_SNB_BI_PARAM_NAME_PREFIX
                                                );
            try
            {
                Class longReadOperationClass = ClassLoaderHelper.loadClass( longReadOperationClassName );
                if ( longReadOperationEnabled )
                {
                    enabledReadOperationTypes.add( longReadOperationClass );
                }
            }
            catch ( ClassLoadingException e )
            {
                throw new WorkloadException(
                        format(
                                "Unable to load operation class for parameter: %s\nGuessed incorrect class name: %s",
                                longReadOperationEnableKey, longReadOperationClassName ),
                        e
                );
            }
        }

        List<String> frequencyKeys = Lists.newArrayList( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_FREQUENCY_KEYS );
        Set<String> missingFrequencyKeys = LdbcSnbBiWorkloadConfiguration.missingParameters( params, frequencyKeys );
        if ( missingFrequencyKeys.isEmpty() )
        {
            // TODO this should really return an initialized instance of LdbcSnbBiWorkloadConfiguration
            // compute interleave based on frequencies
            params = LdbcSnbBiWorkloadConfiguration.convertFrequenciesToInterleaves( params );
        }
        else
        {
            // if any frequencies are not set, there should be specified interleave times for read queries
            Set<String> missingInterleaveKeys = LdbcSnbBiWorkloadConfiguration.missingParameters(
                    params,
                    LdbcSnbBiWorkloadConfiguration.READ_OPERATION_INTERLEAVE_KEYS
            );
            if ( false == missingInterleaveKeys.isEmpty() )
            {
                throw new WorkloadException( format(
                        "Workload could not initialize. One of the following groups of parameters should be set: %s " +
                        "or %s",
                        missingFrequencyKeys.toString(), missingInterleaveKeys.toString() ) );
            }
        }

        try
        {
            readOperation1InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_1_INTERLEAVE_KEY ) );
            readOperation2InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_2_INTERLEAVE_KEY ) );
            readOperation3InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_3_INTERLEAVE_KEY ) );
            readOperation4InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_4_INTERLEAVE_KEY ) );
            readOperation5InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_5_INTERLEAVE_KEY ) );
            readOperation6InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_6_INTERLEAVE_KEY ) );
            readOperation7InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_7_INTERLEAVE_KEY ) );
            readOperation8InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_8_INTERLEAVE_KEY ) );
            readOperation9InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_9_INTERLEAVE_KEY ) );
            readOperation10InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_10_INTERLEAVE_KEY ) );
            readOperation11InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_11_INTERLEAVE_KEY ) );
            readOperation12InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_12_INTERLEAVE_KEY ) );
            readOperation13InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_13_INTERLEAVE_KEY ) );
            readOperation14InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_14_INTERLEAVE_KEY ) );
            readOperation15InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_15_INTERLEAVE_KEY ) );
            readOperation16InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_16_INTERLEAVE_KEY ) );
            readOperation17InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_17_INTERLEAVE_KEY ) );
            readOperation18InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_18_INTERLEAVE_KEY ) );
            readOperation19InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_19_INTERLEAVE_KEY ) );
            readOperation20InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_20_INTERLEAVE_KEY ) );
            readOperation21InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_21_INTERLEAVE_KEY ) );
            readOperation22InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_22_INTERLEAVE_KEY ) );
            readOperation23InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_23_INTERLEAVE_KEY ) );
            readOperation24InterleaveAsMilli =
                    Long.parseLong( params.get( LdbcSnbBiWorkloadConfiguration.READ_OPERATION_24_INTERLEAVE_KEY ) );
        }
        catch ( NumberFormatException e )
        {
            throw new WorkloadException( "Unable to parse one of the read operation interleave values", e );
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
                    new Query1EventStreamReader.Query1Decoder();
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
                    gf.incrementing( workloadStartTimeAsMilli + readOperation1InterleaveAsMilli,
                            readOperation1InterleaveAsMilli );

            readOperation1Stream = gf.assignStartTimes(
                    operation1StartTimes,
                    operation1StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation2Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query2EventStreamReader.Query2Decoder();
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
                    gf.incrementing( workloadStartTimeAsMilli + readOperation2InterleaveAsMilli,
                            readOperation2InterleaveAsMilli );

            readOperation2Stream = gf.assignStartTimes(
                    operation2StartTimes,
                    operation2StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation3Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query3EventStreamReader.Query3Decoder();
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
                    gf.incrementing( workloadStartTimeAsMilli + readOperation3InterleaveAsMilli,
                            readOperation3InterleaveAsMilli );

            readOperation3Stream = gf.assignStartTimes(
                    operation3StartTimes,
                    operation3StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation4Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query4EventStreamReader.Query4Decoder();
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
                    gf.incrementing( workloadStartTimeAsMilli + readOperation4InterleaveAsMilli,
                            readOperation4InterleaveAsMilli );

            readOperation4Stream = gf.assignStartTimes(
                    operation4StartTimes,
                    operation4StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation5Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query5EventStreamReader.Query5Decoder();
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
                    gf.incrementing( workloadStartTimeAsMilli + readOperation5InterleaveAsMilli,
                            readOperation5InterleaveAsMilli );

            readOperation5Stream = gf.assignStartTimes(
                    operation5StartTimes,
                    operation5StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation6Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query6EventStreamReader.Query6Decoder();
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
                    gf.incrementing( workloadStartTimeAsMilli + readOperation6InterleaveAsMilli,
                            readOperation6InterleaveAsMilli );

            readOperation6Stream = gf.assignStartTimes(
                    operation6StartTimes,
                    operation6StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation7Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query7EventStreamReader.Query7Decoder();
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
                    gf.incrementing( workloadStartTimeAsMilli + readOperation7InterleaveAsMilli,
                            readOperation7InterleaveAsMilli );

            readOperation7Stream = gf.assignStartTimes(
                    operation7StartTimes,
                    operation7StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation8Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query8EventStreamReader.Query8Decoder();
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
                    gf.incrementing( workloadStartTimeAsMilli + readOperation8InterleaveAsMilli,
                            readOperation8InterleaveAsMilli );

            readOperation8Stream = gf.assignStartTimes(
                    operation8StartTimes,
                    operation8StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation9Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query9EventStreamReader.Query9Decoder();
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
                    gf.incrementing( workloadStartTimeAsMilli + readOperation9InterleaveAsMilli,
                            readOperation9InterleaveAsMilli );

            readOperation9Stream = gf.assignStartTimes(
                    operation9StartTimes,
                    operation9StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation10Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query10EventStreamReader.Query10Decoder();
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
                    gf.incrementing( workloadStartTimeAsMilli + readOperation10InterleaveAsMilli,
                            readOperation10InterleaveAsMilli );

            readOperation10Stream = gf.assignStartTimes(
                    operation10StartTimes,
                    operation10StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation11Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query11EventStreamReader.Query11Decoder();
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
                    gf.incrementing( workloadStartTimeAsMilli + readOperation11InterleaveAsMilli,
                            readOperation11InterleaveAsMilli );

            readOperation11Stream = gf.assignStartTimes(
                    operation11StartTimes,
                    operation11StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation12Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query12EventStreamReader.Query12Decoder();
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
                    gf.incrementing( workloadStartTimeAsMilli + readOperation12InterleaveAsMilli,
                            readOperation12InterleaveAsMilli );

            readOperation12Stream = gf.assignStartTimes(
                    operation12StartTimes,
                    operation12StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation13Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query13EventStreamReader.Query13Decoder();
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
                    gf.incrementing( workloadStartTimeAsMilli + readOperation13InterleaveAsMilli,
                            readOperation13InterleaveAsMilli );

            readOperation13Stream = gf.assignStartTimes(
                    operation13StartTimes,
                    operation13StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        Iterator<Operation> readOperation14Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query14EventStreamReader.Query14Decoder();
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
                    gf.incrementing( workloadStartTimeAsMilli + readOperation14InterleaveAsMilli,
                            readOperation14InterleaveAsMilli );

            readOperation14Stream = gf.assignStartTimes(
                    operation14StartTimes,
                    operation14StreamWithoutTimes
            );

            readOperationFileReaders.add( charSeeker );
        }

        // TODO add all queries

        if ( enabledReadOperationTypes.contains( LdbcSnbBiQuery1.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation1Stream );
        }
        if ( enabledReadOperationTypes.contains( LdbcSnbBiQuery2.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation2Stream );
        }
        if ( enabledReadOperationTypes.contains( LdbcSnbBiQuery3.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation3Stream );
        }
        if ( enabledReadOperationTypes.contains( LdbcSnbBiQuery4.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation4Stream );
        }
        if ( enabledReadOperationTypes.contains( LdbcSnbBiQuery5.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation5Stream );
        }
        if ( enabledReadOperationTypes.contains( LdbcSnbBiQuery6.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation6Stream );
        }
        if ( enabledReadOperationTypes.contains( LdbcSnbBiQuery7.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation7Stream );
        }
        if ( enabledReadOperationTypes.contains( LdbcSnbBiQuery8.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation8Stream );
        }
        if ( enabledReadOperationTypes.contains( LdbcSnbBiQuery9.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation9Stream );
        }
        if ( enabledReadOperationTypes.contains( LdbcSnbBiQuery10.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation10Stream );
        }
        if ( enabledReadOperationTypes.contains( LdbcSnbBiQuery11.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation11Stream );
        }
        if ( enabledReadOperationTypes.contains( LdbcSnbBiQuery12.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation12Stream );
        }
        if ( enabledReadOperationTypes.contains( LdbcSnbBiQuery13.class ) )
        {
            asynchronousNonDependencyStreamsList.add( readOperation13Stream );
        }
        if ( enabledReadOperationTypes.contains( LdbcSnbBiQuery14.class ) )
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
//        final Set<Class> multiResultOperations = Sets.<Class>newHashSet(
//                LdbcSnbBiQuery1.class,
//                LdbcSnbBiQuery2.class,
//                LdbcSnbBiQuery4.class,
//                LdbcSnbBiQuery5.class,
//                LdbcSnbBiQuery6.class,
//                LdbcSnbBiQuery7.class,
//                LdbcSnbBiQuery8.class,
//                LdbcSnbBiQuery9.class,
//                LdbcSnbBiQuery10.class,
//                LdbcSnbBiQuery11.class,
//                LdbcSnbBiQuery12.class,
//                LdbcSnbBiQuery14.class
//        );
//
//        Integer operationTypeCount = enabledReadOperationTypes.size() + enabledWriteOperationTypes.size();
//        long minimumResultCountPerOperationType = Math.max(
//                1,
//                Math.round( Math.floor(
//                        requiredValidationParameterCount.doubleValue() / operationTypeCount.doubleValue() ) )
//        );
//
//        long writeAddPersonOperationCount = (enabledWriteOperationTypes.contains( LdbcUpdate1AddPerson.class ))
//                                            ? minimumResultCountPerOperationType
//                                            : 0;
//
//        final Map<Class,Long> remainingRequiredResultsPerUpdateType = new HashMap<>();
//        long resultCountsAssignedForUpdateTypesSoFar = 0;
//        for ( Class updateOperationType : enabledWriteOperationTypes )
//        {
//            if ( updateOperationType.equals( LdbcUpdate1AddPerson.class ) )
//            { continue; }
//            remainingRequiredResultsPerUpdateType.put( updateOperationType, minimumResultCountPerOperationType );
//            resultCountsAssignedForUpdateTypesSoFar =
//                    resultCountsAssignedForUpdateTypesSoFar + minimumResultCountPerOperationType;
//        }
//
//        final Map<Class,Long> remainingRequiredResultsPerLongReadType = new HashMap<>();
//        long resultCountsAssignedForLongReadTypesSoFar = 0;
//        for ( Class longReadOperationType : enabledReadOperationTypes )
//        {
//            remainingRequiredResultsPerLongReadType.put( longReadOperationType, minimumResultCountPerOperationType );
//            resultCountsAssignedForLongReadTypesSoFar =
//                    resultCountsAssignedForLongReadTypesSoFar + minimumResultCountPerOperationType;
//        }
//
//        return new LdbcSnbInteractiveDbValidationParametersFilter(
//                multiResultOperations,
//                writeAddPersonOperationCount,
//                remainingRequiredResultsPerUpdateType,
//                remainingRequiredResultsPerLongReadType,
//                enabledShortReadOperationTypes
//        );
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
//        switch ( operation.type() )
//        {
//        case LdbcSnbBiQuery1.TYPE:
//        {
//            LdbcSnbBiQuery1 ldbcQuery = (LdbcSnbBiQuery1) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.personId() );
//            operationAsList.add( ldbcQuery.firstName() );
//            operationAsList.add( ldbcQuery.limit() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcSnbBiQuery2.TYPE:
//        {
//            LdbcSnbBiQuery2 ldbcQuery = (LdbcSnbBiQuery2) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.personId() );
//            operationAsList.add( ldbcQuery.maxDate().getTime() );
//            operationAsList.add( ldbcQuery.limit() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcSnbBiQuery3.TYPE:
//        {
//            LdbcSnbBiQuery3 ldbcQuery = (LdbcSnbBiQuery3) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.personId() );
//            operationAsList.add( ldbcQuery.countryXName() );
//            operationAsList.add( ldbcQuery.countryYName() );
//            operationAsList.add( ldbcQuery.startDate().getTime() );
//            operationAsList.add( ldbcQuery.durationDays() );
//            operationAsList.add( ldbcQuery.limit() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcSnbBiQuery4.TYPE:
//        {
//            LdbcSnbBiQuery4 ldbcQuery = (LdbcSnbBiQuery4) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.personId() );
//            operationAsList.add( ldbcQuery.startDate().getTime() );
//            operationAsList.add( ldbcQuery.durationDays() );
//            operationAsList.add( ldbcQuery.limit() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcSnbBiQuery5.TYPE:
//        {
//            LdbcSnbBiQuery5 ldbcQuery = (LdbcSnbBiQuery5) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.personId() );
//            operationAsList.add( ldbcQuery.minDate().getTime() );
//            operationAsList.add( ldbcQuery.limit() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcSnbBiQuery6.TYPE:
//        {
//            LdbcSnbBiQuery6 ldbcQuery = (LdbcSnbBiQuery6) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.personId() );
//            operationAsList.add( ldbcQuery.tagName() );
//            operationAsList.add( ldbcQuery.limit() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcSnbBiQuery7.TYPE:
//        {
//            LdbcSnbBiQuery7 ldbcQuery = (LdbcSnbBiQuery7) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.personId() );
//            operationAsList.add( ldbcQuery.limit() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcSnbBiQuery8.TYPE:
//        {
//            LdbcSnbBiQuery8 ldbcQuery = (LdbcSnbBiQuery8) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.personId() );
//            operationAsList.add( ldbcQuery.limit() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcSnbBiQuery9.TYPE:
//        {
//            LdbcSnbBiQuery9 ldbcQuery = (LdbcSnbBiQuery9) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.personId() );
//            operationAsList.add( ldbcQuery.maxDate().getTime() );
//            operationAsList.add( ldbcQuery.limit() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcSnbBiQuery10.TYPE:
//        {
//            LdbcSnbBiQuery10 ldbcQuery = (LdbcSnbBiQuery10) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.personId() );
//            operationAsList.add( ldbcQuery.month() );
//            operationAsList.add( ldbcQuery.limit() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcSnbBiQuery11.TYPE:
//        {
//            LdbcSnbBiQuery11 ldbcQuery = (LdbcSnbBiQuery11) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.personId() );
//            operationAsList.add( ldbcQuery.countryName() );
//            operationAsList.add( ldbcQuery.workFromYear() );
//            operationAsList.add( ldbcQuery.limit() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcSnbBiQuery12.TYPE:
//        {
//            LdbcSnbBiQuery12 ldbcQuery = (LdbcSnbBiQuery12) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.personId() );
//            operationAsList.add( ldbcQuery.tagClassName() );
//            operationAsList.add( ldbcQuery.limit() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcSnbBiQuery13.TYPE:
//        {
//            LdbcSnbBiQuery13 ldbcQuery = (LdbcSnbBiQuery13) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.person1Id() );
//            operationAsList.add( ldbcQuery.person2Id() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcSnbBiQuery14.TYPE:
//        {
//            LdbcSnbBiQuery14 ldbcQuery = (LdbcSnbBiQuery14) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.person1Id() );
//            operationAsList.add( ldbcQuery.person2Id() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcShortQuery1PersonProfile.TYPE:
//        {
//            LdbcShortQuery1PersonProfile ldbcQuery = (LdbcShortQuery1PersonProfile) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.personId() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcShortQuery2PersonPosts.TYPE:
//        {
//            LdbcShortQuery2PersonPosts ldbcQuery = (LdbcShortQuery2PersonPosts) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.personId() );
//            operationAsList.add( ldbcQuery.limit() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcShortQuery3PersonFriends.TYPE:
//        {
//            LdbcShortQuery3PersonFriends ldbcQuery = (LdbcShortQuery3PersonFriends) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.personId() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcShortQuery4MessageContent.TYPE:
//        {
//            LdbcShortQuery4MessageContent ldbcQuery = (LdbcShortQuery4MessageContent) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.messageId() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcShortQuery5MessageCreator.TYPE:
//        {
//            LdbcShortQuery5MessageCreator ldbcQuery = (LdbcShortQuery5MessageCreator) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.messageId() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcShortQuery6MessageForum.TYPE:
//        {
//            LdbcShortQuery6MessageForum ldbcQuery = (LdbcShortQuery6MessageForum) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.messageId() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcShortQuery7MessageReplies.TYPE:
//        {
//            LdbcShortQuery7MessageReplies ldbcQuery = (LdbcShortQuery7MessageReplies) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.messageId() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcUpdate1AddPerson.TYPE:
//        {
//            LdbcUpdate1AddPerson ldbcQuery = (LdbcUpdate1AddPerson) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.personId() );
//            operationAsList.add( ldbcQuery.personFirstName() );
//            operationAsList.add( ldbcQuery.personLastName() );
//            operationAsList.add( ldbcQuery.gender() );
//            operationAsList.add( ldbcQuery.birthday().getTime() );
//            operationAsList.add( ldbcQuery.creationDate().getTime() );
//            operationAsList.add( ldbcQuery.locationIp() );
//            operationAsList.add( ldbcQuery.browserUsed() );
//            operationAsList.add( ldbcQuery.cityId() );
//            operationAsList.add( ldbcQuery.languages() );
//            operationAsList.add( ldbcQuery.emails() );
//            operationAsList.add( ldbcQuery.tagIds() );
//            Iterable<Map<String,Object>> studyAt = Lists.newArrayList( Iterables.transform( ldbcQuery.studyAt(),
//                    new Function<LdbcUpdate1AddPerson.Organization,Map<String,Object>>()
//                    {
//                        @Override
//                        public Map<String,Object> apply( LdbcUpdate1AddPerson.Organization organization )
//                        {
//                            Map<String,Object> organizationMap = new HashMap<>();
//                            organizationMap.put( "id", organization.organizationId() );
//                            organizationMap.put( "year", organization.year() );
//                            return organizationMap;
//                        }
//                    } ) );
//            operationAsList.add( studyAt );
//            Iterable<Map<String,Object>> workAt = Lists.newArrayList( Iterables
//                    .transform( ldbcQuery.workAt(), new Function<LdbcUpdate1AddPerson.Organization,Map<String,Object>>()
//                    {
//                        @Override
//                        public Map<String,Object> apply( LdbcUpdate1AddPerson.Organization organization )
//                        {
//                            Map<String,Object> organizationMap = new HashMap<>();
//                            organizationMap.put( "id", organization.organizationId() );
//                            organizationMap.put( "year", organization.year() );
//                            return organizationMap;
//                        }
//                    } ) );
//            operationAsList.add( workAt );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcUpdate2AddPostLike.TYPE:
//        {
//            LdbcUpdate2AddPostLike ldbcQuery = (LdbcUpdate2AddPostLike) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.personId() );
//            operationAsList.add( ldbcQuery.postId() );
//            operationAsList.add( ldbcQuery.creationDate().getTime() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcUpdate3AddCommentLike.TYPE:
//        {
//            LdbcUpdate3AddCommentLike ldbcQuery = (LdbcUpdate3AddCommentLike) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.personId() );
//            operationAsList.add( ldbcQuery.commentId() );
//            operationAsList.add( ldbcQuery.creationDate().getTime() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcUpdate4AddForum.TYPE:
//        {
//            LdbcUpdate4AddForum ldbcQuery = (LdbcUpdate4AddForum) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.forumId() );
//            operationAsList.add( ldbcQuery.forumTitle() );
//            operationAsList.add( ldbcQuery.creationDate().getTime() );
//            operationAsList.add( ldbcQuery.moderatorPersonId() );
//            operationAsList.add( ldbcQuery.tagIds() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcUpdate5AddForumMembership.TYPE:
//        {
//            LdbcUpdate5AddForumMembership ldbcQuery = (LdbcUpdate5AddForumMembership) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.forumId() );
//            operationAsList.add( ldbcQuery.personId() );
//            operationAsList.add( ldbcQuery.joinDate().getTime() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcUpdate6AddPost.TYPE:
//        {
//            LdbcUpdate6AddPost ldbcQuery = (LdbcUpdate6AddPost) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.postId() );
//            operationAsList.add( ldbcQuery.imageFile() );
//            operationAsList.add( ldbcQuery.creationDate().getTime() );
//            operationAsList.add( ldbcQuery.locationIp() );
//            operationAsList.add( ldbcQuery.browserUsed() );
//            operationAsList.add( ldbcQuery.language() );
//            operationAsList.add( ldbcQuery.content() );
//            operationAsList.add( ldbcQuery.length() );
//            operationAsList.add( ldbcQuery.authorPersonId() );
//            operationAsList.add( ldbcQuery.forumId() );
//            operationAsList.add( ldbcQuery.countryId() );
//            operationAsList.add( ldbcQuery.tagIds() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcUpdate7AddComment.TYPE:
//        {
//            LdbcUpdate7AddComment ldbcQuery = (LdbcUpdate7AddComment) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.commentId() );
//            operationAsList.add( ldbcQuery.creationDate() );
//            operationAsList.add( ldbcQuery.locationIp() );
//            operationAsList.add( ldbcQuery.browserUsed() );
//            operationAsList.add( ldbcQuery.content() );
//            operationAsList.add( ldbcQuery.length() );
//            operationAsList.add( ldbcQuery.authorPersonId() );
//            operationAsList.add( ldbcQuery.countryId() );
//            operationAsList.add( ldbcQuery.replyToPostId() );
//            operationAsList.add( ldbcQuery.replyToCommentId() );
//            operationAsList.add( ldbcQuery.tagIds() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        case LdbcUpdate8AddFriendship.TYPE:
//        {
//            LdbcUpdate8AddFriendship ldbcQuery = (LdbcUpdate8AddFriendship) operation;
//            List<Object> operationAsList = new ArrayList<>();
//            operationAsList.add( ldbcQuery.getClass().getName() );
//            operationAsList.add( ldbcQuery.person1Id() );
//            operationAsList.add( ldbcQuery.person2Id() );
//            operationAsList.add( ldbcQuery.creationDate().getTime() );
//            try
//            {
//                return OBJECT_MAPPER.writeValueAsString( operationAsList );
//            }
//            catch ( IOException e )
//            {
//                throw new SerializingMarshallingException(
//                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
//            }
//        }
//        default:
//        {
//            throw new SerializingMarshallingException(
//                    format(
//                            "Workload does not know how to serialize operation\nWorkload: %s\nOperation Type: " +
//                            "%s\nOperation: %s",
//                            getClass().getName(),
//                            operation.getClass().getName(),
//                            operation ) );
//        }
//        }
    }

    @Override
    public Operation marshalOperation( String serializedOperation ) throws SerializingMarshallingException
    {
        // TODO implement
        // TODO move marshalling logic to static methods on operation classes
        return null;
//        List<Object> operationAsList;
//        try
//        {
//            operationAsList = OBJECT_MAPPER.readValue( serializedOperation, TYPE_REFERENCE );
//        }
//        catch ( IOException e )
//        {
//            throw new SerializingMarshallingException(
//                    format( "Error while parsing serialized results\n%s", serializedOperation ), e );
//        }
//
//        String operationTypeName = (String) operationAsList.get( 0 );
//
//        if ( operationTypeName.equals( LdbcSnbBiQuery1.class.getName() ) )
//        {
//            long personId = ((Number) operationAsList.get( 1 )).longValue();
//            String firstName = (String) operationAsList.get( 2 );
//            int limit = ((Number) operationAsList.get( 3 )).intValue();
//            return new LdbcSnbBiQuery1( personId, firstName, limit );
//        }
//
//        if ( operationTypeName.equals( LdbcSnbBiQuery2.class.getName() ) )
//        {
//            long personId = ((Number) operationAsList.get( 1 )).longValue();
//            Date maxDate = new Date( ((Number) operationAsList.get( 2 )).longValue() );
//            int limit = ((Number) operationAsList.get( 3 )).intValue();
//            return new LdbcSnbBiQuery2( personId, maxDate, limit );
//        }
//
//        if ( operationTypeName.equals( LdbcSnbBiQuery3.class.getName() ) )
//        {
//            long personId = ((Number) operationAsList.get( 1 )).longValue();
//            String countryXName = (String) operationAsList.get( 2 );
//            String countryYName = (String) operationAsList.get( 3 );
//            Date startDate = new Date( ((Number) operationAsList.get( 4 )).longValue() );
//            int durationDays = ((Number) operationAsList.get( 5 )).intValue();
//            int limit = ((Number) operationAsList.get( 6 )).intValue();
//            return new LdbcSnbBiQuery3( personId, countryXName, countryYName, startDate, durationDays, limit );
//        }
//
//        if ( operationTypeName.equals( LdbcSnbBiQuery4.class.getName() ) )
//        {
//            long personId = ((Number) operationAsList.get( 1 )).longValue();
//            Date startDate = new Date( ((Number) operationAsList.get( 2 )).longValue() );
//            int durationDays = ((Number) operationAsList.get( 3 )).intValue();
//            int limit = ((Number) operationAsList.get( 4 )).intValue();
//            return new LdbcSnbBiQuery4( personId, startDate, durationDays, limit );
//        }
//
//        if ( operationTypeName.equals( LdbcSnbBiQuery5.class.getName() ) )
//        {
//            long personId = ((Number) operationAsList.get( 1 )).longValue();
//            Date minDate = new Date( ((Number) operationAsList.get( 2 )).longValue() );
//            int limit = ((Number) operationAsList.get( 3 )).intValue();
//            return new LdbcSnbBiQuery5( personId, minDate, limit );
//        }
//
//        if ( operationTypeName.equals( LdbcSnbBiQuery6.class.getName() ) )
//        {
//            long personId = ((Number) operationAsList.get( 1 )).longValue();
//            String tagName = (String) operationAsList.get( 2 );
//            int limit = ((Number) operationAsList.get( 3 )).intValue();
//            return new LdbcSnbBiQuery6( personId, tagName, limit );
//        }
//
//        if ( operationTypeName.equals( LdbcSnbBiQuery7.class.getName() ) )
//        {
//            long personId = ((Number) operationAsList.get( 1 )).longValue();
//            int limit = ((Number) operationAsList.get( 2 )).intValue();
//            return new LdbcSnbBiQuery7( personId, limit );
//        }
//
//        if ( operationTypeName.equals( LdbcSnbBiQuery8.class.getName() ) )
//        {
//            long personId = ((Number) operationAsList.get( 1 )).longValue();
//            int limit = ((Number) operationAsList.get( 2 )).intValue();
//            return new LdbcSnbBiQuery8( personId, limit );
//        }
//
//        if ( operationTypeName.equals( LdbcSnbBiQuery9.class.getName() ) )
//        {
//            long personId = ((Number) operationAsList.get( 1 )).longValue();
//            Date maxDate = new Date( ((Number) operationAsList.get( 2 )).longValue() );
//            int limit = ((Number) operationAsList.get( 3 )).intValue();
//            return new LdbcSnbBiQuery9( personId, maxDate, limit );
//        }
//
//        if ( operationTypeName.equals( LdbcSnbBiQuery10.class.getName() ) )
//        {
//            long personId = ((Number) operationAsList.get( 1 )).longValue();
//            int month = ((Number) operationAsList.get( 2 )).intValue();
//            int limit = ((Number) operationAsList.get( 3 )).intValue();
//            return new LdbcSnbBiQuery10( personId, month, limit );
//        }
//
//        if ( operationTypeName.equals( LdbcSnbBiQuery11.class.getName() ) )
//        {
//            long personId = ((Number) operationAsList.get( 1 )).longValue();
//            String countryName = (String) operationAsList.get( 2 );
//            int workFromYear = ((Number) operationAsList.get( 3 )).intValue();
//            int limit = ((Number) operationAsList.get( 4 )).intValue();
//            return new LdbcSnbBiQuery11( personId, countryName, workFromYear, limit );
//        }
//
//        if ( operationTypeName.equals( LdbcSnbBiQuery12.class.getName() ) )
//        {
//            long personId = ((Number) operationAsList.get( 1 )).longValue();
//            String tagClassName = (String) operationAsList.get( 2 );
//            int limit = ((Number) operationAsList.get( 3 )).intValue();
//            return new LdbcSnbBiQuery12( personId, tagClassName, limit );
//        }
//
//        if ( operationTypeName.equals( LdbcSnbBiQuery13.class.getName() ) )
//        {
//            long person1Id = ((Number) operationAsList.get( 1 )).longValue();
//            long person2Id = ((Number) operationAsList.get( 2 )).longValue();
//            return new LdbcSnbBiQuery13( person1Id, person2Id );
//        }
//
//        if ( operationTypeName.equals( LdbcSnbBiQuery14.class.getName() ) )
//        {
//            long person1Id = ((Number) operationAsList.get( 1 )).longValue();
//            long person2Id = ((Number) operationAsList.get( 2 )).longValue();
//            return new LdbcSnbBiQuery14( person1Id, person2Id );
//        }
//
//        if ( operationTypeName.equals( LdbcShortQuery1PersonProfile.class.getName() ) )
//        {
//            long personId = ((Number) operationAsList.get( 1 )).longValue();
//            return new LdbcShortQuery1PersonProfile( personId );
//        }
//
//        if ( operationTypeName.equals( LdbcShortQuery2PersonPosts.class.getName() ) )
//        {
//            long personId = ((Number) operationAsList.get( 1 )).longValue();
//            int limit = ((Number) operationAsList.get( 2 )).intValue();
//            return new LdbcShortQuery2PersonPosts( personId, limit );
//        }
//
//        if ( operationTypeName.equals( LdbcShortQuery3PersonFriends.class.getName() ) )
//        {
//            long personId = ((Number) operationAsList.get( 1 )).longValue();
//            return new LdbcShortQuery3PersonFriends( personId );
//        }
//
//        if ( operationTypeName.equals( LdbcShortQuery4MessageContent.class.getName() ) )
//        {
//            long messageId = ((Number) operationAsList.get( 1 )).longValue();
//            return new LdbcShortQuery4MessageContent( messageId );
//        }
//
//        if ( operationTypeName.equals( LdbcShortQuery5MessageCreator.class.getName() ) )
//        {
//            long messageId = ((Number) operationAsList.get( 1 )).longValue();
//            return new LdbcShortQuery5MessageCreator( messageId );
//        }
//
//        if ( operationTypeName.equals( LdbcShortQuery6MessageForum.class.getName() ) )
//        {
//            long messageId = ((Number) operationAsList.get( 1 )).longValue();
//            return new LdbcShortQuery6MessageForum( messageId );
//        }
//
//        if ( operationTypeName.equals( LdbcShortQuery7MessageReplies.class.getName() ) )
//        {
//            long messageId = ((Number) operationAsList.get( 1 )).longValue();
//            return new LdbcShortQuery7MessageReplies( messageId );
//        }
//
//        if ( operationTypeName.equals( LdbcUpdate1AddPerson.class.getName() ) )
//        {
//            long personId = ((Number) operationAsList.get( 1 )).longValue();
//            String personFirstName = (String) operationAsList.get( 2 );
//            String personLastName = (String) operationAsList.get( 3 );
//            String gender = (String) operationAsList.get( 4 );
//            Date birthday = new Date( ((Number) operationAsList.get( 5 )).longValue() );
//            Date creationDate = new Date( ((Number) operationAsList.get( 6 )).longValue() );
//            String locationIp = (String) operationAsList.get( 7 );
//            String browserUsed = (String) operationAsList.get( 8 );
//            long cityId = ((Number) operationAsList.get( 9 )).longValue();
//            List<String> languages = (List<String>) operationAsList.get( 10 );
//            List<String> emails = (List<String>) operationAsList.get( 11 );
//            List<Long> tagIds = Lists.newArrayList(
//                    Iterables.transform( (List<Number>) operationAsList.get( 12 ), new Function<Number,Long>()
//                    {
//                        @Override
//                        public Long apply( Number number )
//                        {
//                            return number.longValue();
//                        }
//                    } ) );
//            List<Map<String,Object>> studyAtList = (List<Map<String,Object>>) operationAsList.get( 13 );
//            List<LdbcUpdate1AddPerson.Organization> studyAt = Lists.newArrayList( Iterables
//                    .transform( studyAtList, new Function<Map<String,Object>,LdbcUpdate1AddPerson.Organization>()
//                    {
//                        @Override
//                        public LdbcUpdate1AddPerson.Organization apply( Map<String,Object> input )
//                        {
//                            long organizationId = ((Number) input.get( "id" )).longValue();
//                            int year = ((Number) input.get( "year" )).intValue();
//                            return new LdbcUpdate1AddPerson.Organization( organizationId, year );
//                        }
//                    } ) );
//            List<Map<String,Object>> workAtList = (List<Map<String,Object>>) operationAsList.get( 14 );
//            List<LdbcUpdate1AddPerson.Organization> workAt = Lists.newArrayList( Iterables
//                    .transform( workAtList, new Function<Map<String,Object>,LdbcUpdate1AddPerson.Organization>()
//                    {
//                        @Override
//                        public LdbcUpdate1AddPerson.Organization apply( Map<String,Object> input )
//                        {
//                            long organizationId = ((Number) input.get( "id" )).longValue();
//                            int year = ((Number) input.get( "year" )).intValue();
//                            return new LdbcUpdate1AddPerson.Organization( organizationId, year );
//                        }
//                    } ) );
//
//            return new LdbcUpdate1AddPerson( personId, personFirstName, personLastName, gender, birthday, creationDate,
//                    locationIp, browserUsed, cityId, languages, emails, tagIds, studyAt, workAt );
//        }
//
//        if ( operationTypeName.equals( LdbcUpdate2AddPostLike.class.getName() ) )
//        {
//            long personId = ((Number) operationAsList.get( 1 )).longValue();
//            long postId = ((Number) operationAsList.get( 2 )).longValue();
//            Date creationDate = new Date( ((Number) operationAsList.get( 3 )).longValue() );
//
//            return new LdbcUpdate2AddPostLike( personId, postId, creationDate );
//        }
//
//        if ( operationTypeName.equals( LdbcUpdate3AddCommentLike.class.getName() ) )
//        {
//            long personId = ((Number) operationAsList.get( 1 )).longValue();
//            long commentId = ((Number) operationAsList.get( 2 )).longValue();
//            Date creationDate = new Date( ((Number) operationAsList.get( 3 )).longValue() );
//
//            return new LdbcUpdate3AddCommentLike( personId, commentId, creationDate );
//        }
//
//        if ( operationTypeName.equals( LdbcUpdate4AddForum.class.getName() ) )
//        {
//            long forumId = ((Number) operationAsList.get( 1 )).longValue();
//            String forumTitle = (String) operationAsList.get( 2 );
//            Date creationDate = new Date( ((Number) operationAsList.get( 3 )).longValue() );
//            long moderatorPersonId = ((Number) operationAsList.get( 4 )).longValue();
//            List<Long> tagIds = Lists.newArrayList(
//                    Iterables.transform( (List<Number>) operationAsList.get( 5 ), new Function<Number,Long>()
//                    {
//
//                        @Override
//                        public Long apply( Number number )
//                        {
//                            return number.longValue();
//                        }
//                    } ) );
//
//            return new LdbcUpdate4AddForum( forumId, forumTitle, creationDate, moderatorPersonId, tagIds );
//        }
//
//
//        if ( operationTypeName.equals( LdbcUpdate5AddForumMembership.class.getName() ) )
//        {
//            long forumId = ((Number) operationAsList.get( 1 )).longValue();
//            long personId = ((Number) operationAsList.get( 2 )).longValue();
//            Date creationDate = new Date( ((Number) operationAsList.get( 3 )).longValue() );
//
//            return new LdbcUpdate5AddForumMembership( forumId, personId, creationDate );
//        }
//
//        if ( operationTypeName.equals( LdbcUpdate6AddPost.class.getName() ) )
//        {
//            long postId = ((Number) operationAsList.get( 1 )).longValue();
//            String imageFile = (String) operationAsList.get( 2 );
//            Date creationDate = new Date( ((Number) operationAsList.get( 3 )).longValue() );
//            String locationIp = (String) operationAsList.get( 4 );
//            String browserUsed = (String) operationAsList.get( 5 );
//            String language = (String) operationAsList.get( 6 );
//            String content = (String) operationAsList.get( 7 );
//            int length = ((Number) operationAsList.get( 8 )).intValue();
//            long authorPersonId = ((Number) operationAsList.get( 9 )).longValue();
//            long forumId = ((Number) operationAsList.get( 10 )).longValue();
//            long countryId = ((Number) operationAsList.get( 11 )).longValue();
//            List<Long> tagIds = Lists.newArrayList(
//                    Iterables.transform( (List<Number>) operationAsList.get( 12 ), new Function<Number,Long>()
//                    {
//                        @Override
//                        public Long apply( Number number )
//                        {
//                            return number.longValue();
//                        }
//                    } ) );
//
//            return new LdbcUpdate6AddPost( postId, imageFile, creationDate, locationIp, browserUsed, language, content,
//                    length, authorPersonId, forumId, countryId, tagIds );
//        }
//
//        if ( operationTypeName.equals( LdbcUpdate7AddComment.class.getName() ) )
//        {
//            long commentId = ((Number) operationAsList.get( 1 )).longValue();
//            Date creationDate = new Date( ((Number) operationAsList.get( 2 )).longValue() );
//            String locationIp = (String) operationAsList.get( 3 );
//            String browserUsed = (String) operationAsList.get( 4 );
//            String content = (String) operationAsList.get( 5 );
//            int length = ((Number) operationAsList.get( 6 )).intValue();
//            long authorPersonId = ((Number) operationAsList.get( 7 )).longValue();
//            long countryId = ((Number) operationAsList.get( 8 )).longValue();
//            long replyToPostId = ((Number) operationAsList.get( 9 )).longValue();
//            long replyToCommentId = ((Number) operationAsList.get( 10 )).longValue();
//            List<Long> tagIds = Lists.newArrayList(
//                    Iterables.transform( (List<Number>) operationAsList.get( 11 ), new Function<Number,Long>()
//                    {
//                        @Override
//                        public Long apply( Number number )
//                        {
//                            return number.longValue();
//                        }
//                    } ) );
//
//            return new LdbcUpdate7AddComment( commentId, creationDate, locationIp, browserUsed, content, length,
//                    authorPersonId, countryId, replyToPostId, replyToCommentId, tagIds );
//        }
//
//        if ( operationTypeName.equals( LdbcUpdate8AddFriendship.class.getName() ) )
//        {
//            long person1Id = ((Number) operationAsList.get( 1 )).longValue();
//            long person2Id = ((Number) operationAsList.get( 2 )).longValue();
//            Date creationDate = new Date( ((Number) operationAsList.get( 3 )).longValue() );
//
//            return new LdbcUpdate8AddFriendship( person1Id, person2Id, creationDate );
//        }
//
//        throw new SerializingMarshallingException(
//                format(
//                        "Workload does not know how to marshal operation\nWorkload: %s\nAssumed Operation Type: " +
//                        "%s\nSerialized Operation: %s",
//                        getClass().getName(),
//                        operationTypeName,
//                        serializedOperation ) );
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
