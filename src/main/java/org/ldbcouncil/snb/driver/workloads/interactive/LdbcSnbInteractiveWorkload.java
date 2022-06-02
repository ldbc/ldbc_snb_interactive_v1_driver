package org.ldbcouncil.snb.driver.workloads.interactive;

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import org.ldbcouncil.snb.driver.ChildOperationGenerator;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.Workload;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.WorkloadStreams;
import org.ldbcouncil.snb.driver.control.ConsoleAndFileDriverConfiguration;
import org.ldbcouncil.snb.driver.control.OperationMode;
import org.ldbcouncil.snb.driver.csv.CsvLoader;
import org.ldbcouncil.snb.driver.csv.DuckDbConnectionState;
import org.ldbcouncil.snb.driver.csv.charseeker.BufferedCharSeeker;
import org.ldbcouncil.snb.driver.csv.charseeker.Extractors;
import org.ldbcouncil.snb.driver.csv.charseeker.Readables;
import org.ldbcouncil.snb.driver.csv.charseeker.ThreadAheadReadable;
import org.ldbcouncil.snb.driver.csv.simple.SimpleCsvFileReader;
import org.ldbcouncil.snb.driver.generator.GeneratorFactory;
import org.ldbcouncil.snb.driver.generator.QueryEventStreamReader;
import org.ldbcouncil.snb.driver.generator.RandomDataGeneratorFactory;
import org.ldbcouncil.snb.driver.util.ClassLoaderHelper;
import org.ldbcouncil.snb.driver.util.ClassLoadingException;
import org.ldbcouncil.snb.driver.util.MapUtils;
import org.ldbcouncil.snb.driver.util.Tuple;
import org.ldbcouncil.snb.driver.util.Tuple2;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.*;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class LdbcSnbInteractiveWorkload extends Workload
{
    private List<Closeable> forumUpdateOperationsFileReaders = new ArrayList<>();
    private List<File> forumUpdateOperationFiles = new ArrayList<>();
    private List<Closeable> personUpdateOperationsFileReaders = new ArrayList<>();
    private List<File> personUpdateOperationFiles = new ArrayList<>();

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

    private long updateInterleaveAsMilli;
    private double compressionRatio;
    private double shortReadDissipationFactor;

    private Set<Class> enabledLongReadOperationTypes;
    private Set<Class> enabledShortReadOperationTypes;
    private Set<Class> enabledWriteOperationTypes;
    private LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser parser;

    @Override
    public Map<Integer,Class<? extends Operation>> operationTypeToClassMapping()
    {
        return LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping();
    }

    @Override
    public void onInit( Map<String,String> params ) throws WorkloadException
    {
        List<String> compulsoryKeys = Lists.newArrayList();

        // Validation mode does not require parameter directory
        if (params.containsKey(ConsoleAndFileDriverConfiguration.MODE_ARG)
            && OperationMode.valueOf(params.get(ConsoleAndFileDriverConfiguration.MODE_ARG)) != OperationMode.validate_database )
        {
            compulsoryKeys.add( LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY );
        }

        compulsoryKeys.addAll( LdbcSnbInteractiveWorkloadConfiguration.LONG_READ_OPERATION_ENABLE_KEYS );
        compulsoryKeys.addAll( LdbcSnbInteractiveWorkloadConfiguration.WRITE_OPERATION_ENABLE_KEYS );
        compulsoryKeys.addAll( LdbcSnbInteractiveWorkloadConfiguration.SHORT_READ_OPERATION_ENABLE_KEYS );

        Set<String> missingPropertyParameters =
                LdbcSnbInteractiveWorkloadConfiguration.missingParameters( params, compulsoryKeys );
        if ( false == missingPropertyParameters.isEmpty() )
        {
            throw new WorkloadException( format( "Workload could not initialize due to missing parameters: %s",
                    missingPropertyParameters.toString() ) );
        }

        if ( params.containsKey( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY ) )
        {
            String updatesDirectoryPath =
                    params.get( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY ).trim();
            File updatesDirectory = new File( updatesDirectoryPath );
            if ( false == updatesDirectory.exists() )
            {
                throw new WorkloadException( format( "Updates directory does not exist\nDirectory: %s",
                        updatesDirectory.getAbsolutePath() ) );
            }
            if ( false == updatesDirectory.isDirectory() )
            {
                throw new WorkloadException( format( "Updates directory is not a directory\nDirectory: %s",
                        updatesDirectory.getAbsolutePath() ) );
            }
            forumUpdateOperationFiles = LdbcSnbInteractiveWorkloadConfiguration
                    .forumUpdateFilesInDirectory( updatesDirectory );
            personUpdateOperationFiles =
                    LdbcSnbInteractiveWorkloadConfiguration.personUpdateFilesInDirectory( updatesDirectory );
        }
        else
        {
            forumUpdateOperationFiles = new ArrayList<>();
            personUpdateOperationFiles = new ArrayList<>();
        }

        File parametersDir =
                new File( params.get( LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY ).trim() );
        if ( false == parametersDir.exists() )
        {
            throw new WorkloadException(
                    format( "Parameters directory does not exist: %s", parametersDir.getAbsolutePath() ) );
        }
        for ( String readOperationParamsFilename :
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_PARAMS_FILENAMES )
        {
            File readOperationParamsFile = new File( parametersDir, readOperationParamsFilename );
            if ( false == readOperationParamsFile.exists() )
            {
                throw new WorkloadException(
                        format( "Read operation parameters file does not exist: %s",
                                readOperationParamsFile.getAbsolutePath() ) );
            }
        }
        readOperation1File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_1_PARAMS_FILENAME );
        readOperation2File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_2_PARAMS_FILENAME );
        readOperation3File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3_PARAMS_FILENAME );
        readOperation4File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_4_PARAMS_FILENAME );
        readOperation5File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_5_PARAMS_FILENAME );
        readOperation7File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_7_PARAMS_FILENAME );
        readOperation8File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_8_PARAMS_FILENAME );
        readOperation9File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_9_PARAMS_FILENAME );
        readOperation6File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_6_PARAMS_FILENAME );
        readOperation10File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_10_PARAMS_FILENAME );
        readOperation11File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_11_PARAMS_FILENAME );
        readOperation12File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_12_PARAMS_FILENAME );
        readOperation13File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13_PARAMS_FILENAME );
        readOperation14File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14_PARAMS_FILENAME );

        enabledLongReadOperationTypes = new HashSet<>();
        for ( String longReadOperationEnableKey : LdbcSnbInteractiveWorkloadConfiguration
                .LONG_READ_OPERATION_ENABLE_KEYS )
        {
            String longReadOperationEnabledString = params.get( longReadOperationEnableKey ).trim();
            Boolean longReadOperationEnabled = Boolean.parseBoolean( longReadOperationEnabledString );
            String longReadOperationClassName =
                    LdbcSnbInteractiveWorkloadConfiguration.LDBC_INTERACTIVE_PACKAGE_PREFIX +
                    LdbcSnbInteractiveWorkloadConfiguration.removePrefix(
                            LdbcSnbInteractiveWorkloadConfiguration.removeSuffix(
                                    longReadOperationEnableKey,
                                    LdbcSnbInteractiveWorkloadConfiguration.ENABLE_SUFFIX
                            ),
                            LdbcSnbInteractiveWorkloadConfiguration
                                    .LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX
                    );
            try
            {
                Class longReadOperationClass = ClassLoaderHelper.loadClass( longReadOperationClassName );
                if ( longReadOperationEnabled )
                { 
                    enabledLongReadOperationTypes.add( longReadOperationClass );
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

        enabledShortReadOperationTypes = new HashSet<>();
        for ( String shortReadOperationEnableKey : LdbcSnbInteractiveWorkloadConfiguration
                .SHORT_READ_OPERATION_ENABLE_KEYS )
        {
            String shortReadOperationEnabledString = params.get( shortReadOperationEnableKey ).trim();
            Boolean shortReadOperationEnabled = Boolean.parseBoolean( shortReadOperationEnabledString );
            String shortReadOperationClassName =
                    LdbcSnbInteractiveWorkloadConfiguration.LDBC_INTERACTIVE_PACKAGE_PREFIX +
                    LdbcSnbInteractiveWorkloadConfiguration.removePrefix(
                            LdbcSnbInteractiveWorkloadConfiguration.removeSuffix(
                                    shortReadOperationEnableKey,
                                    LdbcSnbInteractiveWorkloadConfiguration.ENABLE_SUFFIX
                            ),
                            LdbcSnbInteractiveWorkloadConfiguration
                                    .LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX
                    );
            try
            {
                Class shortReadOperationClass = ClassLoaderHelper.loadClass( shortReadOperationClassName );
                if ( shortReadOperationEnabled )
                { 
                    enabledShortReadOperationTypes.add( shortReadOperationClass ); 
                }

            }
            catch ( ClassLoadingException e )
            {
                throw new WorkloadException(
                        format(
                                "Unable to load operation class for parameter: %s\nGuessed incorrect class name: %s",
                                shortReadOperationEnableKey, shortReadOperationClassName ),
                        e
                );
            }
        }
        if ( false == enabledShortReadOperationTypes.isEmpty() )
        {
            if ( false == params.containsKey( LdbcSnbInteractiveWorkloadConfiguration.SHORT_READ_DISSIPATION ) )
            {
                throw new WorkloadException( format( "Configuration parameter missing: %s",
                        LdbcSnbInteractiveWorkloadConfiguration.SHORT_READ_DISSIPATION ) );
            }
            shortReadDissipationFactor = Double.parseDouble(
                    params.get( LdbcSnbInteractiveWorkloadConfiguration.SHORT_READ_DISSIPATION ).trim()
            );
            if ( shortReadDissipationFactor < 0 || shortReadDissipationFactor > 1 )
            {
                throw new WorkloadException(
                        format( "Configuration parameter %s should be in interval [1.0,0.0] but is: %s",
                                shortReadDissipationFactor ) );
            }
        }

        enabledWriteOperationTypes = new HashSet<>();
        for ( String writeOperationEnableKey : LdbcSnbInteractiveWorkloadConfiguration.WRITE_OPERATION_ENABLE_KEYS )
        {
            String writeOperationEnabledString = params.get( writeOperationEnableKey ).trim();
            Boolean writeOperationEnabled = Boolean.parseBoolean( writeOperationEnabledString );
            String writeOperationClassName = LdbcSnbInteractiveWorkloadConfiguration.LDBC_INTERACTIVE_PACKAGE_PREFIX +
                                             LdbcSnbInteractiveWorkloadConfiguration.removePrefix(
                                                     LdbcSnbInteractiveWorkloadConfiguration.removeSuffix(
                                                             writeOperationEnableKey,
                                                             LdbcSnbInteractiveWorkloadConfiguration.ENABLE_SUFFIX
                                                     ),
                                                     LdbcSnbInteractiveWorkloadConfiguration
                                                             .LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX
                                             );
            try
            {
                Class writeOperationClass = ClassLoaderHelper.loadClass( writeOperationClassName );
                if ( writeOperationEnabled )
                { 
                    enabledWriteOperationTypes.add( writeOperationClass ); 
                }

            }
            catch ( ClassLoadingException e )
            {
                throw new WorkloadException(
                        format(
                                "Unable to load operation class for parameter: %s\nGuessed incorrect class name: %s",
                                writeOperationEnableKey, writeOperationClassName ),
                        e
                );
            }
        }

        // First load the scale factor from the provided properties file, then load the frequency keys from resources
        if (!params.containsKey(LdbcSnbInteractiveWorkloadConfiguration.SCALE_FACTOR))
        {
            // if SCALE_FACTOR is missing but writes are enabled it is an error
            throw new WorkloadException(
                format( "Workload could not initialize. Missing parameter: %s",
                LdbcSnbInteractiveWorkloadConfiguration.SCALE_FACTOR ) );
        }

        String scaleFactor = params.get( LdbcSnbInteractiveWorkloadConfiguration.SCALE_FACTOR ).trim();
        // Load the frequencyKeys for the appropiate scale factor if that scale factor is supported
        
        String scaleFactorPropertiesPath = "configuration/ldbc/snb/interactive/sf" + scaleFactor  + ".properties"; 
        // Load the properties file, throw error if file is not present (and thus not supported)
        final Properties scaleFactorProperties = new Properties();

        try (final InputStream stream =
            this.getClass().getClassLoader().getResourceAsStream(scaleFactorPropertiesPath)) {
                scaleFactorProperties.load(stream);
        }
        catch (IOException e){
            throw new WorkloadException(
                        format( "Workload could not initialize. Scale factor %s not supported. %s",
                        scaleFactor, e));
        }

        Map<String,String> tempFileParams = MapUtils.propertiesToMap( scaleFactorProperties );
        Map<String,String> tmp = new HashMap<String,String>(tempFileParams);

        // Check if validation params creation is used. If so, set the frequencies to 1
        if ( OperationMode.valueOf(params.get(ConsoleAndFileDriverConfiguration.MODE_ARG)) == OperationMode.create_validation )
        {
            Map<String, String> freqs = new HashMap<String, String>();
            String updateInterleave = tmp.get(LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE);
            freqs.put(LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE, updateInterleave);
            freqs.put(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_1_FREQUENCY_KEY, "1");
            freqs.put(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_2_FREQUENCY_KEY, "1");
            freqs.put(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3_FREQUENCY_KEY, "1");
            freqs.put(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_4_FREQUENCY_KEY, "1");
            freqs.put(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_5_FREQUENCY_KEY, "1");
            freqs.put(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_6_FREQUENCY_KEY, "1");
            freqs.put(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_7_FREQUENCY_KEY, "1");
            freqs.put(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_8_FREQUENCY_KEY, "1");
            freqs.put(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_9_FREQUENCY_KEY, "1");
            freqs.put(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_10_FREQUENCY_KEY, "1");
            freqs.put(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_11_FREQUENCY_KEY, "1");
            freqs.put(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_12_FREQUENCY_KEY, "1");
            freqs.put(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13_FREQUENCY_KEY, "1");
            freqs.put(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14_FREQUENCY_KEY, "1");
            freqs.keySet().removeAll(params.keySet());
            params.putAll(freqs);
        }
        else
        {
            tmp.keySet().removeAll(params.keySet());
            params.putAll(tmp);
        }

        List<String> frequencyKeys =
                Lists.newArrayList( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_FREQUENCY_KEYS );
        Set<String> missingFrequencyKeys = LdbcSnbInteractiveWorkloadConfiguration
                .missingParameters( params, frequencyKeys );

        if ( enabledWriteOperationTypes.isEmpty() &&
             false == params.containsKey( LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE ) )
        {
            // if UPDATE_INTERLEAVE is missing and writes are disabled set it to DEFAULT
            params.put(
                    LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE,
                    LdbcSnbInteractiveWorkloadConfiguration.DEFAULT_UPDATE_INTERLEAVE
            );
        }
        if ( false == params.containsKey( LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE ) )
        {
            // if UPDATE_INTERLEAVE is missing but writes are enabled it is an error
            throw new WorkloadException(
                    format( "Workload could not initialize. Missing parameter: %s",
                            LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE ) );
        }
        updateInterleaveAsMilli =
                Integer.parseInt( params.get( LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE ).trim() );

        if ( missingFrequencyKeys.isEmpty() )
        {
            // all frequency arguments were given, compute interleave based on frequencies
            params = LdbcSnbInteractiveWorkloadConfiguration.convertFrequenciesToInterleaves( params );
        }
        else
        {
            // if any frequencies are not set, there should be specified interleave times for read queries
            Set<String> missingInterleaveKeys = LdbcSnbInteractiveWorkloadConfiguration.missingParameters(
                    params,
                    LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_INTERLEAVE_KEYS
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
            readOperation1InterleaveAsMilli = Long.parseLong(
                    params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_1_INTERLEAVE_KEY ).trim() );
            readOperation2InterleaveAsMilli = Long.parseLong(
                    params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_2_INTERLEAVE_KEY ).trim() );
            readOperation3InterleaveAsMilli = Long.parseLong(
                    params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3_INTERLEAVE_KEY ).trim() );
            readOperation4InterleaveAsMilli = Long.parseLong(
                    params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_4_INTERLEAVE_KEY ).trim() );
            readOperation5InterleaveAsMilli = Long.parseLong(
                    params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_5_INTERLEAVE_KEY ).trim() );
            readOperation6InterleaveAsMilli = Long.parseLong(
                    params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_6_INTERLEAVE_KEY ).trim() );
            readOperation7InterleaveAsMilli = Long.parseLong(
                    params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_7_INTERLEAVE_KEY ).trim() );
            readOperation8InterleaveAsMilli = Long.parseLong(
                    params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_8_INTERLEAVE_KEY ).trim() );
            readOperation9InterleaveAsMilli = Long.parseLong(
                    params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_9_INTERLEAVE_KEY ).trim() );
            readOperation10InterleaveAsMilli = Long.parseLong(
                    params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_10_INTERLEAVE_KEY ).trim() );
            readOperation11InterleaveAsMilli = Long.parseLong(
                    params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_11_INTERLEAVE_KEY ).trim() );
            readOperation12InterleaveAsMilli = Long.parseLong(
                    params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_12_INTERLEAVE_KEY ).trim() );
            readOperation13InterleaveAsMilli = Long.parseLong(
                    params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13_INTERLEAVE_KEY ).trim() );
            readOperation14InterleaveAsMilli = Long.parseLong(
                    params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14_INTERLEAVE_KEY ).trim() );
        }
        catch ( NumberFormatException e )
        {
            throw new WorkloadException( "Unable to parse one of the read operation interleave values", e );
        }

        String parserString = params.get( LdbcSnbInteractiveWorkloadConfiguration.UPDATE_STREAM_PARSER );
        if ( null == parserString )
        {
            parserString = LdbcSnbInteractiveWorkloadConfiguration.DEFAULT_UPDATE_STREAM_PARSER.name();
        }
        if ( false == LdbcSnbInteractiveWorkloadConfiguration.isValidParser( parserString ) )
        {
            throw new WorkloadException( "Invalid parser: " + parserString );
        }
        this.parser = LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser.valueOf( parserString.trim() );
        this.compressionRatio = Double.parseDouble(
                params.get( ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG ).trim()
        );
    }

    @Override
    synchronized protected void onClose() throws IOException
    {
        for ( Closeable forumUpdateOperationsFileReader : forumUpdateOperationsFileReaders )
        {
            forumUpdateOperationsFileReader.close();
        }

        for ( Closeable personUpdateOperationsFileReader : personUpdateOperationsFileReaders )
        {
            personUpdateOperationsFileReader.close();
        }

        for ( Closeable readOperationFileReader : readOperationFileReaders )
        {
            readOperationFileReader.close();
        }
    }

    private Tuple2<Iterator<Operation>,Closeable> fileToWriteStreamParser( File updateOperationsFile,
            LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser parser ) throws IOException, WorkloadException
    {
        switch ( parser )
        {
        case REGEX:
        {
            SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader( updateOperationsFile,
                    SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING );
            return Tuple.<Iterator<Operation>,Closeable>tuple2( WriteEventStreamReaderRegex.create( csvFileReader ),
                    csvFileReader );
        }
        case CHAR_SEEKER:
        {
            int bufferSize = 1 * 1024 * 1024;
//                BufferedCharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new FileReader
// (updateOperationsFile)), bufferSize);
            BufferedCharSeeker charSeeker = new BufferedCharSeeker(
                    Readables.wrap(
                            new InputStreamReader( new FileInputStream( updateOperationsFile ), Charsets.UTF_8 )
                    ),
                    bufferSize
            );
            Extractors extractors = new Extractors( ';', ',' );
            return Tuple.<Iterator<Operation>,Closeable>tuple2(
                    WriteEventStreamReaderCharSeeker.create( charSeeker, extractors, '|' ), charSeeker );
        }
        case CHAR_SEEKER_THREAD:
        {
            int bufferSize = 1 * 1024 * 1024;
            BufferedCharSeeker charSeeker = new BufferedCharSeeker(
                    ThreadAheadReadable.threadAhead(
                            Readables.wrap(
                                    new InputStreamReader( new FileInputStream( updateOperationsFile ), Charsets.UTF_8 )
                            ),
                            bufferSize
                    ),
                    bufferSize
            );
            Extractors extractors = new Extractors( ';', ',' );
            return Tuple.<Iterator<Operation>,Closeable>tuple2(
                    WriteEventStreamReaderCharSeeker.create( charSeeker, extractors, '|' ), charSeeker );
        }
        }
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader( updateOperationsFile,
                SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING );
        return Tuple.<Iterator<Operation>,Closeable>tuple2( WriteEventStreamReaderRegex.create( csvFileReader ),
                csvFileReader );
    }

    @Override
    protected WorkloadStreams getStreams( GeneratorFactory gf, boolean hasDbConnected ) throws WorkloadException
    {
        long workloadStartTimeAsMilli = Long.MAX_VALUE;
        WorkloadStreams ldbcSnbInteractiveWorkloadStreams = new WorkloadStreams();
        List<Iterator<?>> asynchronousDependencyStreamsList = new ArrayList<>();
        List<Iterator<?>> asynchronousNonDependencyStreamsList = new ArrayList<>();
        Set<Class<? extends Operation>> dependentAsynchronousOperationTypes = Sets.newHashSet();
        Set<Class<? extends Operation>> dependencyAsynchronousOperationTypes = Sets.newHashSet();

        /* *******
         * *******
         * *******
         *  WRITES
         * *******
         * *******
         * *******/

        // TODO put person/forum update stream pairs into same streams, to half required thread count
         /*
         * Create person write operation streams
         */
        if ( enabledWriteOperationTypes.contains( LdbcUpdate1AddPerson.class ) )
        {
            for ( File personUpdateOperationFile : personUpdateOperationFiles )
            {
                Iterator<Operation> personUpdateOperationsParser;
                try
                {
                    Tuple2<Iterator<Operation>,Closeable> parserAndCloseable =
                            fileToWriteStreamParser( personUpdateOperationFile, parser );
                    personUpdateOperationsParser = parserAndCloseable._1();
                    personUpdateOperationsFileReaders.add( parserAndCloseable._2() );
                }
                catch ( IOException e )
                {
                    throw new WorkloadException(
                            "Unable to open person update stream: " + personUpdateOperationFile.getAbsolutePath(), e );
                }
                if ( false == personUpdateOperationsParser.hasNext() )
                {
                    // Update stream is empty
                    System.out.println(
                            format( ""
                                    + "***********************************************\n"
                                    + "  !! WARNING !!\n"
                                    + "  Update stream is empty: %s\n"
                                    + "  Check that data generation process completed successfully\n"
                                    + "***********************************************",
                                    personUpdateOperationFile.getAbsolutePath()
                            )
                    );
                    continue;
                }
                PeekingIterator<Operation> unfilteredPersonUpdateOperations =
                        Iterators.peekingIterator( personUpdateOperationsParser );

                try
                {
                    if ( unfilteredPersonUpdateOperations.peek().scheduledStartTimeAsMilli() <
                         workloadStartTimeAsMilli )
                    {
                        workloadStartTimeAsMilli = unfilteredPersonUpdateOperations.peek().scheduledStartTimeAsMilli();
                    }
                }
                catch ( NoSuchElementException e )
                {
                    // do nothing, exception just means that stream was empty
                }

                // Filter Write Operations
                Predicate<Operation> enabledWriteOperationsFilter = new Predicate<Operation>()
                {
                    @Override
                    public boolean apply( Operation operation )
                    {
                        return enabledWriteOperationTypes.contains( operation.getClass() );
                    }
                };
                Iterator<Operation> filteredPersonUpdateOperations =
                        Iterators.filter( unfilteredPersonUpdateOperations, enabledWriteOperationsFilter );

                Set<Class<? extends Operation>> dependentPersonUpdateOperationTypes = Sets.newHashSet();
                Set<Class<? extends Operation>> dependencyPersonUpdateOperationTypes =
                        Sets.<Class<? extends Operation>>newHashSet(
                                LdbcUpdate1AddPerson.class
                        );

                ChildOperationGenerator personUpdateChildOperationGenerator = null;

                ldbcSnbInteractiveWorkloadStreams.addBlockingStream(
                        dependentPersonUpdateOperationTypes,
                        dependencyPersonUpdateOperationTypes,
                        filteredPersonUpdateOperations,
                        Collections.<Operation>emptyIterator(),
                        personUpdateChildOperationGenerator
                );
            }
        }

        /*
         * Create forum write operation streams
         */
        if ( enabledWriteOperationTypes.contains( LdbcUpdate2AddPostLike.class ) ||
             enabledWriteOperationTypes.contains( LdbcUpdate3AddCommentLike.class ) ||
             enabledWriteOperationTypes.contains( LdbcUpdate4AddForum.class ) ||
             enabledWriteOperationTypes.contains( LdbcUpdate5AddForumMembership.class ) ||
             enabledWriteOperationTypes.contains( LdbcUpdate6AddPost.class ) ||
             enabledWriteOperationTypes.contains( LdbcUpdate7AddComment.class ) ||
             enabledWriteOperationTypes.contains( LdbcUpdate8AddFriendship.class )
                )
        {
            for ( File forumUpdateOperationFile : forumUpdateOperationFiles )
            {
                Iterator<Operation> forumUpdateOperationsParser;
                try
                {
                    Tuple2<Iterator<Operation>,Closeable> parserAndCloseable =
                            fileToWriteStreamParser( forumUpdateOperationFile, parser );
                    forumUpdateOperationsParser = parserAndCloseable._1();
                    forumUpdateOperationsFileReaders.add( parserAndCloseable._2() );
                }
                catch ( IOException e )
                {
                    throw new WorkloadException(
                            "Unable to open forum update stream: " + forumUpdateOperationFile.getAbsolutePath(), e );
                }
                if ( false == forumUpdateOperationsParser.hasNext() )
                {
                    // Update stream is empty
                    System.out.println(
                            format( ""
                                    + "***********************************************\n"
                                    + "  !! WARNING !!\n"
                                    + "  Update stream is empty: %s\n"
                                    + "  Check that data generation process completed successfully\n"
                                    + "***********************************************",
                                    forumUpdateOperationFile.getAbsolutePath()
                            )
                    );
                    continue;
                }
                PeekingIterator<Operation> unfilteredForumUpdateOperations =
                        Iterators.peekingIterator( forumUpdateOperationsParser );

                try
                {
                    if ( unfilteredForumUpdateOperations.peek().scheduledStartTimeAsMilli() < workloadStartTimeAsMilli )
                    {
                        workloadStartTimeAsMilli = unfilteredForumUpdateOperations.peek().scheduledStartTimeAsMilli();
                    }
                }
                catch ( NoSuchElementException e )
                {
                    // do nothing, exception just means that stream was empty
                }

                // Filter Write Operations
                Predicate<Operation> enabledWriteOperationsFilter = new Predicate<Operation>()
                {
                    @Override
                    public boolean apply( Operation operation )
                    {
                        return enabledWriteOperationTypes.contains( operation.getClass() );
                    }
                };
                Iterator<Operation> filteredForumUpdateOperations =
                        Iterators.filter( unfilteredForumUpdateOperations, enabledWriteOperationsFilter );

                Set<Class<? extends Operation>> dependentForumUpdateOperationTypes =
                        Sets.<Class<? extends Operation>>newHashSet(
                                LdbcUpdate2AddPostLike.class,
                                LdbcUpdate3AddCommentLike.class,
                                LdbcUpdate4AddForum.class,
                                LdbcUpdate5AddForumMembership.class,
                                LdbcUpdate6AddPost.class,
                                LdbcUpdate7AddComment.class,
                                LdbcUpdate8AddFriendship.class
                        );
                Set<Class<? extends Operation>> dependencyForumUpdateOperationTypes = Sets.newHashSet();

                ChildOperationGenerator forumUpdateChildOperationGenerator = null;

                ldbcSnbInteractiveWorkloadStreams.addBlockingStream(
                        dependentForumUpdateOperationTypes,
                        dependencyForumUpdateOperationTypes,
                        Collections.<Operation>emptyIterator(),
                        filteredForumUpdateOperations,
                        forumUpdateChildOperationGenerator
                );
            }
        }

        if ( Long.MAX_VALUE == workloadStartTimeAsMilli )
        { workloadStartTimeAsMilli = 0; }

        /* *******
         * *******
         * *******
         *  LONG READS
         * *******
         * *******
         * *******/

         /*
         * Create read operation streams, with specified interleaves
         */
        CsvLoader loader;
        try {
            DuckDbConnectionState db = new DuckDbConnectionState();
            loader = new CsvLoader(db);
        }
        catch (SQLException e){
            throw new WorkloadException(format("Error creating loader for operation streams %s", e));
        }
        ReadOperationStream readOperationStream = new ReadOperationStream(gf, workloadStartTimeAsMilli, loader);
        
        QueryEventStreamReader.EventDecoder<Object[]> decoder1 = new Query1EventStreamReader.QueryDecoder();
        Iterator<Operation> readOperation1Stream = readOperationStream.readOperationStream(Query1EventStreamReader.class, decoder1, readOperation1InterleaveAsMilli, readOperation1File);

        QueryEventStreamReader.EventDecoder<Object[]> decoder2 = new Query2EventStreamReader.QueryDecoder();
        Iterator<Operation> readOperation2Stream = readOperationStream.readOperationStream(Query2EventStreamReader.class, decoder2, readOperation2InterleaveAsMilli, readOperation2File);

        QueryEventStreamReader.EventDecoder<Object[]> decoder3 = new Query3EventStreamReader.QueryDecoder();
        Iterator<Operation> readOperation3Stream = readOperationStream.readOperationStream(Query3EventStreamReader.class, decoder3, readOperation3InterleaveAsMilli, readOperation3File);

        QueryEventStreamReader.EventDecoder<Object[]> decoder4 = new Query4EventStreamReader.QueryDecoder();
        Iterator<Operation> readOperation4Stream = readOperationStream.readOperationStream(Query4EventStreamReader.class, decoder4, readOperation4InterleaveAsMilli, readOperation4File);

        QueryEventStreamReader.EventDecoder<Object[]> decoder5 = new Query5EventStreamReader.QueryDecoder();
        Iterator<Operation> readOperation5Stream = readOperationStream.readOperationStream(Query5EventStreamReader.class, decoder5, readOperation5InterleaveAsMilli, readOperation5File);

        QueryEventStreamReader.EventDecoder<Object[]> decoder6 = new Query6EventStreamReader.QueryDecoder();
        Iterator<Operation> readOperation6Stream = readOperationStream.readOperationStream(Query6EventStreamReader.class, decoder6, readOperation6InterleaveAsMilli, readOperation6File);

        QueryEventStreamReader.EventDecoder<Object[]> decoder7 = new Query7EventStreamReader.QueryDecoder();
        Iterator<Operation> readOperation7Stream = readOperationStream.readOperationStream(Query7EventStreamReader.class, decoder7, readOperation7InterleaveAsMilli, readOperation7File);

        QueryEventStreamReader.EventDecoder<Object[]> decoder8 = new Query8EventStreamReader.QueryDecoder();
        Iterator<Operation> readOperation8Stream = readOperationStream.readOperationStream(Query8EventStreamReader.class, decoder8, readOperation8InterleaveAsMilli, readOperation8File);

        QueryEventStreamReader.EventDecoder<Object[]> decoder9 = new Query9EventStreamReader.QueryDecoder();
        Iterator<Operation> readOperation9Stream = readOperationStream.readOperationStream(Query9EventStreamReader.class, decoder9, readOperation9InterleaveAsMilli, readOperation9File);

        QueryEventStreamReader.EventDecoder<Object[]> decoder10 = new Query10EventStreamReader.QueryDecoder();
        Iterator<Operation> readOperation10Stream = readOperationStream.readOperationStream(Query10EventStreamReader.class, decoder10, readOperation10InterleaveAsMilli, readOperation10File);

        QueryEventStreamReader.EventDecoder<Object[]> decoder11 = new Query11EventStreamReader.QueryDecoder();
        Iterator<Operation> readOperation11Stream = readOperationStream.readOperationStream(Query11EventStreamReader.class, decoder11, readOperation11InterleaveAsMilli, readOperation11File);

        QueryEventStreamReader.EventDecoder<Object[]> decoder12 = new Query12EventStreamReader.QueryDecoder();
        Iterator<Operation> readOperation12Stream = readOperationStream.readOperationStream(Query12EventStreamReader.class, decoder12, readOperation12InterleaveAsMilli, readOperation12File);

        QueryEventStreamReader.EventDecoder<Object[]> decoder13 = new Query13EventStreamReader.QueryDecoder();
        Iterator<Operation> readOperation13Stream = readOperationStream.readOperationStream(Query13EventStreamReader.class, decoder13, readOperation13InterleaveAsMilli, readOperation13File);

        QueryEventStreamReader.EventDecoder<Object[]> decoder14 = new Query14EventStreamReader.QueryDecoder();
        Iterator<Operation> readOperation14Stream = readOperationStream.readOperationStream(Query14EventStreamReader.class, decoder14, readOperation14InterleaveAsMilli, readOperation14File);

        if ( enabledLongReadOperationTypes.contains( LdbcQuery1.class ) )
        { asynchronousNonDependencyStreamsList.add( readOperation1Stream ); }
        if ( enabledLongReadOperationTypes.contains( LdbcQuery2.class ) )
        { asynchronousNonDependencyStreamsList.add( readOperation2Stream ); }
        if ( enabledLongReadOperationTypes.contains( LdbcQuery3.class ) )
        { asynchronousNonDependencyStreamsList.add( readOperation3Stream ); }
        if ( enabledLongReadOperationTypes.contains( LdbcQuery4.class ) )
        { asynchronousNonDependencyStreamsList.add( readOperation4Stream ); }
        if ( enabledLongReadOperationTypes.contains( LdbcQuery5.class ) )
        { asynchronousNonDependencyStreamsList.add( readOperation5Stream ); }
        if ( enabledLongReadOperationTypes.contains( LdbcQuery6.class ) )
        { asynchronousNonDependencyStreamsList.add( readOperation6Stream ); }
        if ( enabledLongReadOperationTypes.contains( LdbcQuery7.class ) )
        { asynchronousNonDependencyStreamsList.add( readOperation7Stream ); }
        if ( enabledLongReadOperationTypes.contains( LdbcQuery8.class ) )
        { asynchronousNonDependencyStreamsList.add( readOperation8Stream ); }
        if ( enabledLongReadOperationTypes.contains( LdbcQuery9.class ) )
        { asynchronousNonDependencyStreamsList.add( readOperation9Stream ); }
        if ( enabledLongReadOperationTypes.contains( LdbcQuery10.class ) )
        { asynchronousNonDependencyStreamsList.add( readOperation10Stream ); }
        if ( enabledLongReadOperationTypes.contains( LdbcQuery11.class ) )
        { asynchronousNonDependencyStreamsList.add( readOperation11Stream ); }
        if ( enabledLongReadOperationTypes.contains( LdbcQuery12.class ) )
        { asynchronousNonDependencyStreamsList.add( readOperation12Stream ); }
        if ( enabledLongReadOperationTypes.contains( LdbcQuery13.class ) )
        { asynchronousNonDependencyStreamsList.add( readOperation13Stream ); }
        if ( enabledLongReadOperationTypes.contains( LdbcQuery14.class ) )
        { asynchronousNonDependencyStreamsList.add( readOperation14Stream ); }

        /*
         * Merge all dependency asynchronous operation streams, ordered by operation start times
         */
        Iterator<Operation> asynchronousDependencyStreams = gf.mergeSortOperationsByTimeStamp(
                asynchronousDependencyStreamsList.toArray( new Iterator[asynchronousDependencyStreamsList.size()] )
        );
        /*
         * Merge all non dependency asynchronous operation streams, ordered by operation start times
         */
        Iterator<Operation> asynchronousNonDependencyStreams = gf.mergeSortOperationsByTimeStamp(
                asynchronousNonDependencyStreamsList
                        .toArray( new Iterator[asynchronousNonDependencyStreamsList.size()] )
        );

        /* *******
         * *******
         *  SHORT READS
         * *******
         * *******/

        ChildOperationGenerator shortReadsChildGenerator = null;
        if ( false == enabledShortReadOperationTypes.isEmpty() )
        {
            Map<Integer,Long> longReadInterleavesAsMilli = new HashMap<>();
            longReadInterleavesAsMilli.put( LdbcQuery1.TYPE, readOperation1InterleaveAsMilli );
            longReadInterleavesAsMilli.put( LdbcQuery2.TYPE, readOperation2InterleaveAsMilli );
            longReadInterleavesAsMilli.put( LdbcQuery3.TYPE, readOperation3InterleaveAsMilli );
            longReadInterleavesAsMilli.put( LdbcQuery4.TYPE, readOperation4InterleaveAsMilli );
            longReadInterleavesAsMilli.put( LdbcQuery5.TYPE, readOperation5InterleaveAsMilli );
            longReadInterleavesAsMilli.put( LdbcQuery6.TYPE, readOperation6InterleaveAsMilli );
            longReadInterleavesAsMilli.put( LdbcQuery7.TYPE, readOperation7InterleaveAsMilli );
            longReadInterleavesAsMilli.put( LdbcQuery8.TYPE, readOperation8InterleaveAsMilli );
            longReadInterleavesAsMilli.put( LdbcQuery9.TYPE, readOperation9InterleaveAsMilli );
            longReadInterleavesAsMilli.put( LdbcQuery10.TYPE, readOperation10InterleaveAsMilli );
            longReadInterleavesAsMilli.put( LdbcQuery11.TYPE, readOperation11InterleaveAsMilli );
            longReadInterleavesAsMilli.put( LdbcQuery12.TYPE, readOperation12InterleaveAsMilli );
            longReadInterleavesAsMilli.put( LdbcQuery13.TYPE, readOperation13InterleaveAsMilli );
            longReadInterleavesAsMilli.put( LdbcQuery14.TYPE, readOperation14InterleaveAsMilli );

            RandomDataGeneratorFactory randomFactory = new RandomDataGeneratorFactory( 42l );
            double initialProbability = 1.0;
            Queue<Long> personIdBuffer = (hasDbConnected)
                                         ? LdbcSnbShortReadGenerator.synchronizedCircularQueueBuffer( 1024 )
                                         : LdbcSnbShortReadGenerator.constantBuffer( 1 );
            Queue<Long> messageIdBuffer = (hasDbConnected)
                                          ? LdbcSnbShortReadGenerator.synchronizedCircularQueueBuffer( 1024 )
                                          : LdbcSnbShortReadGenerator.constantBuffer( 1 );
            LdbcSnbShortReadGenerator.SCHEDULED_START_TIME_POLICY scheduledStartTimePolicy = (hasDbConnected)
                                                                                             ?
                                                                                             LdbcSnbShortReadGenerator.SCHEDULED_START_TIME_POLICY.PREVIOUS_OPERATION_ACTUAL_FINISH_TIME
                                                                                             :
                                                                                             LdbcSnbShortReadGenerator.SCHEDULED_START_TIME_POLICY.PREVIOUS_OPERATION_SCHEDULED_START_TIME;
            LdbcSnbShortReadGenerator.BufferReplenishFun bufferReplenishFun = (hasDbConnected)
                                                                              ? new LdbcSnbShortReadGenerator
                    .ResultBufferReplenishFun(
                    personIdBuffer, messageIdBuffer )
                                                                              : new LdbcSnbShortReadGenerator
                                                                                      .NoOpBufferReplenishFun();
            shortReadsChildGenerator = new LdbcSnbShortReadGenerator(
                    initialProbability,
                    shortReadDissipationFactor,
                    updateInterleaveAsMilli,
                    enabledShortReadOperationTypes,
                    compressionRatio,
                    personIdBuffer,
                    messageIdBuffer,
                    randomFactory,
                    longReadInterleavesAsMilli,
                    scheduledStartTimePolicy,
                    bufferReplenishFun
            );
        }

        /* **************
         * **************
         *  FINAL STREAMS
         * **************
         * **************/

        ldbcSnbInteractiveWorkloadStreams.setAsynchronousStream(
                dependentAsynchronousOperationTypes,
                dependencyAsynchronousOperationTypes,
                asynchronousDependencyStreams,
                asynchronousNonDependencyStreams,
                shortReadsChildGenerator
        );

        return ldbcSnbInteractiveWorkloadStreams;
    }

    /**
     * Creates the validation parameter filter, which determines the amount of validation parameters
     * @param requiredValidationParameterCount The total validation parameters to create
     */
    @Override
    public DbValidationParametersFilter dbValidationParametersFilter( Integer requiredValidationParameterCount )
    {
        Integer operationTypeCount = enabledLongReadOperationTypes.size() + enabledWriteOperationTypes.size();

        // Calculate amount of validation operations to create
        long minimumResultCountPerOperationType = Math.max(
                1,
                Math.round( Math.floor(
                        requiredValidationParameterCount.doubleValue() / operationTypeCount.doubleValue() ) )
        );

        long writeAddPersonOperationCount = 0;
        if (enabledWriteOperationTypes.contains( LdbcUpdate1AddPerson.class ))
        {
            writeAddPersonOperationCount = minimumResultCountPerOperationType;
        }

        final Map<Class,Long> remainingRequiredResultsPerUpdateType = new HashMap<>();
        for ( Class updateOperationType : enabledWriteOperationTypes )
        {
            if ( updateOperationType.equals( LdbcUpdate1AddPerson.class ) )
            { continue; }
            remainingRequiredResultsPerUpdateType.put( updateOperationType, minimumResultCountPerOperationType );
        }

        final Map<Class,Long> remainingRequiredResultsPerLongReadType = new HashMap<>();
        for ( Class longReadOperationType : enabledLongReadOperationTypes )
        {
            remainingRequiredResultsPerLongReadType.put( longReadOperationType, minimumResultCountPerOperationType );
        }

        return new LdbcSnbInteractiveDbValidationParametersFilter(
                writeAddPersonOperationCount,
                remainingRequiredResultsPerUpdateType,
                remainingRequiredResultsPerLongReadType,
                enabledShortReadOperationTypes
        );
    }

    @Override
    public int enabledValidationOperations()
    {
        return enabledLongReadOperationTypes.size() + enabledWriteOperationTypes.size();
    }

    @Override
    public long maxExpectedInterleaveAsMilli()
    {
        return TimeUnit.HOURS.toMillis( 1 );
    }
}
