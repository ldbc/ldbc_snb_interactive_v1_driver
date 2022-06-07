package org.ldbcouncil.snb.driver.workloads.interactive;

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
import org.ldbcouncil.snb.driver.generator.GeneratorFactory;
import org.ldbcouncil.snb.driver.generator.RandomDataGeneratorFactory;
import org.ldbcouncil.snb.driver.generator.UpdateEventStreamDecoder.UpdateEventDecoder;
import org.ldbcouncil.snb.driver.util.ClassLoaderHelper;
import org.ldbcouncil.snb.driver.util.ClassLoadingException;
import org.ldbcouncil.snb.driver.util.MapUtils;

import org.ldbcouncil.snb.driver.workloads.interactive.UpdateEventStreamReader.*;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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
    private List<File> forumUpdateOperationFiles = new ArrayList<>();
    private List<File> personUpdateOperationFiles = new ArrayList<>();

    private Map<Integer,Long> longReadInterleavesAsMilli;

    private long updateInterleaveAsMilli;
    private double compressionRatio;
    private double shortReadDissipationFactor;
    private OperationMode operationMode;
    private File parametersDir;
    private int numThreads;
    
    private Set<Class> enabledLongReadOperationTypes;
    private Set<Class> enabledShortReadOperationTypes;
    private Set<Class> enabledWriteOperationTypes;

    @Override
    public Map<Integer,Class<? extends Operation>> operationTypeToClassMapping()
    {
        return LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping();
    }

    @Override
    public void onInit( Map<String,String> params ) throws WorkloadException
    {
        List<String> compulsoryKeys = Lists.newArrayList();

        if (params.containsKey(ConsoleAndFileDriverConfiguration.THREADS_ARG)){
            numThreads = Integer.parseInt(params.get(ConsoleAndFileDriverConfiguration.THREADS_ARG));
        }
        else
        {
            numThreads = Integer.parseInt(params.get(ConsoleAndFileDriverConfiguration.THREADS_DEFAULT_STRING));
        }

        // Check operation mode, default is execute_benchmark
        if (params.containsKey(ConsoleAndFileDriverConfiguration.MODE_ARG)){
            operationMode = OperationMode.valueOf(params.get(ConsoleAndFileDriverConfiguration.MODE_ARG));
        }

        // Validation mode does not require parameter directory
        if (operationMode != OperationMode.validate_database )
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

        if (operationMode != OperationMode.validate_database)
        {
            parametersDir = new File( params.get( LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY ).trim() );
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

        }
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
            longReadInterleavesAsMilli = new HashMap<>();
            longReadInterleavesAsMilli.put( LdbcQuery1.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_1_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery2.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_2_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery3.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery4.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_4_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery5.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_5_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery6.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_6_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery7.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_7_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery8.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_8_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery9.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_9_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery10.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_10_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery11.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_11_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery12.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_12_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery13.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery14.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14_INTERLEAVE_KEY ).trim() ) );

        }
        catch ( NumberFormatException e )
        {
            throw new WorkloadException( "Unable to parse one of the read operation interleave values", e );
        }

        this.compressionRatio = Double.parseDouble(
                params.get( ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG ).trim()
        );
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

        CsvLoader loader;
        try {
            DuckDbConnectionState db = new DuckDbConnectionState();
            loader = new CsvLoader(db);
        }
        catch (SQLException e){
            throw new WorkloadException(format("Error creating loader for operation streams %s", e));
        }

        Map<Integer,UpdateEventDecoder<Operation>> decoders = new HashMap<>();
            decoders.put(1, new EventDecoderAddPerson());
            decoders.put(2, new EventDecoderAddLikePost());
            decoders.put(3, new EventDecoderAddLikeComment());
            decoders.put(4, new EventDecoderAddForum());
            decoders.put(5, new EventDecoderAddForumMembership());
            decoders.put(6, new EventDecoderAddPost());
            decoders.put(7, new EventDecoderAddComment());
            decoders.put(8, new EventDecoderAddFriendship());

        /* *******
         * *******
         *  WRITES
         * *******
         * *******/
        File personUpdateOperationFile = null;
        if ( enabledWriteOperationTypes.contains( LdbcUpdate1AddPerson.class ) && !personUpdateOperationFiles.isEmpty())
        {
            personUpdateOperationFile = personUpdateOperationFiles.get(0);
        }

        File forumUpdateOperationFile = null;
        if ( !forumUpdateOperationFiles.isEmpty() && (
             enabledWriteOperationTypes.contains( LdbcUpdate2AddPostLike.class ) ||
             enabledWriteOperationTypes.contains( LdbcUpdate3AddCommentLike.class ) ||
             enabledWriteOperationTypes.contains( LdbcUpdate4AddForum.class ) ||
             enabledWriteOperationTypes.contains( LdbcUpdate5AddForumMembership.class ) ||
             enabledWriteOperationTypes.contains( LdbcUpdate6AddPost.class ) ||
             enabledWriteOperationTypes.contains( LdbcUpdate7AddComment.class ) ||
             enabledWriteOperationTypes.contains( LdbcUpdate8AddFriendship.class ))
        )
        {
            forumUpdateOperationFile = forumUpdateOperationFiles.get(0);
        }

        if (personUpdateOperationFile != null || forumUpdateOperationFile != null)
        {
            List<Iterator<Operation>> updateOperationStreams = getCombinedUpdateStreamIterators(
                personUpdateOperationFile,
                forumUpdateOperationFile,
                loader, gf, decoders
            );
            workloadStartTimeAsMilli = addUpdateOperationStreamsToWorkload(
                workloadStartTimeAsMilli,
                updateOperationStreams,
                ldbcSnbInteractiveWorkloadStreams
            );
        }

        if ( Long.MAX_VALUE == workloadStartTimeAsMilli )
        { 
            workloadStartTimeAsMilli = 0;
        }

        /* *******
         * *******
         *  LONG READS
         * *******
         * *******/
        asynchronousNonDependencyStreamsList = getOperationStreams(gf, workloadStartTimeAsMilli,loader);
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
            shortReadsChildGenerator = getShortReadGenerator(hasDbConnected);
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
                asynchronousNonDependencyStreams, // This is empty at loading time
                shortReadsChildGenerator
        );

        return ldbcSnbInteractiveWorkloadStreams;
    }

    /**
     * Create Short read operations
     * @param hasDbConnected
     * @return
     */
    private LdbcSnbShortReadGenerator getShortReadGenerator(boolean hasDbConnected)
    {
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
        return new LdbcSnbShortReadGenerator(
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


    /**
     * Loads the update stream: updateStream_0_0_person.csv for the persons and
     * updateStream_0_0_forum.csv for the  update events.
     * @param updateOperationFile The updateStream csv file to load
     * @param loader The loader to read the csv-file
     * @param decoders HashMap of decoders for each event type
     * @return Loaded operation stream (Iterator<Operation>)
     * @throws WorkloadException When there is an error opening the update stream
     */
    private Iterator<Operation> getUpdateStreamIterator(
        File updateOperationFile,
        CsvLoader loader,
        Map<Integer,UpdateEventDecoder<Operation>> decoders
    ) throws WorkloadException
    {
        Iterator<Operation> updateOperationsParser;
        try
        {
            UpdateOperationStream updateOperationStream = new UpdateOperationStream(loader);
            updateOperationsParser = updateOperationStream.readUpdateStream(updateOperationFile, new UpdateEventStreamReader.EventDecoder(decoders));
        }
        catch ( WorkloadException e )
        {
            throw new WorkloadException(
                    "Unable to open update stream: " + updateOperationFile.getAbsolutePath(), e );
        }
        if ( false == updateOperationsParser.hasNext() )
        {
            // Update stream is empty
            System.out.println(
                    format( ""
                            + "***********************************************\n"
                            + "  !! WARNING !!\n"
                            + "  Update stream is empty: %s\n"
                            + "  Check that data generation process completed successfully\n"
                            + "***********************************************",
                            updateOperationFile.getAbsolutePath()
                    )
            );
        }
        return updateOperationsParser;
    }

    /**
     * Combines the two update streams and split the streams to the number of threads
     * set by the user.
     * @param personUpdateOperationFile The person updateStream
     * @param forumUpdateOperationFile The forum updateStream
     * @param loader The loader to read the csv-file
     * @param gf The GeneratorFactory object to use for merge operation streams
     * @param decoders HashMap of decoders for each event type
     * @return List of operation streams
     * @throws WorkloadException When one of the operation streams fails to load
     */
    private List<Iterator<Operation>> getCombinedUpdateStreamIterators(
        File personUpdateOperationFile,
        File forumUpdateOperationFile,
        CsvLoader loader,
        GeneratorFactory gf,
        Map<Integer,UpdateEventDecoder<Operation>> decoders
        ) throws WorkloadException
    {
        Iterator<Operation> personUpdateOperationsParser;
        if (personUpdateOperationFile != null) {
            personUpdateOperationsParser = getUpdateStreamIterator(personUpdateOperationFile, loader, decoders);
        }
        else{
            personUpdateOperationsParser = Collections.<Operation>emptyIterator();
        }
        Iterator<Operation> forumUpdateOperationsParser;
        if (forumUpdateOperationFile != null) {
            forumUpdateOperationsParser = getUpdateStreamIterator(forumUpdateOperationFile, loader, decoders);
        }
        else {
            forumUpdateOperationsParser = Collections.<Operation>emptyIterator();
        }

        // Merge two streams
        // mergeSortOperationsByScheduledStartTime or mergeSortOperationsByTimeStamp?
        Iterator<Operation> operationStreams = gf.mergeSortOperationsByScheduledStartTime(personUpdateOperationsParser, forumUpdateOperationsParser);
        List<Iterator<Operation>> operationIterators = new ArrayList<>();
        if (numThreads == 1)
        {
            operationIterators.add(operationStreams);
            return operationIterators;
        }
        List<ArrayList<Operation>> operationLists = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            // Instantiate lists
            operationLists.add(new ArrayList<Operation>());
        }

        int index = 0;
        // Split accros threads
        while(operationStreams.hasNext())
        {
            int listIndex = index % numThreads;
            Operation operation = operationStreams.next();
            operationLists.get(listIndex).add(operation);
        }

        for (ArrayList<Operation> operationList : operationLists) {
            operationIterators.add(operationList.iterator());
        }
        return operationIterators;
    }

    /**
     * Add operation streams to workloadstreams and retrieve the workloadStartTimeAsMilli.
     * @param workloadStartTimeAsMilli The current workloadStartTimeAsMilli
     * @param operationStreams List of operation streams, where length list is number of threads
     * @param ldbcSnbInteractiveWorkloadStreams The workload object to add the streams
     * @return Updated workloadStartTimeAsMilli
     * @throws WorkloadException
     */
    private long addUpdateOperationStreamsToWorkload(
        long workloadStartTimeAsMilli,
        List<Iterator<Operation>> operationStreams,
        WorkloadStreams ldbcSnbInteractiveWorkloadStreams
    ) throws WorkloadException
    {
        for (Iterator<Operation> iterator : operationStreams) {
            PeekingIterator<Operation> unfilteredUpdateOperations=  Iterators.peekingIterator( iterator );
            try
            {
                if ( unfilteredUpdateOperations.peek().scheduledStartTimeAsMilli() < workloadStartTimeAsMilli )
                {
                    workloadStartTimeAsMilli = unfilteredUpdateOperations.peek().scheduledStartTimeAsMilli();
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
            Iterator<Operation> filteredUpdateOperations =
                    Iterators.filter( unfilteredUpdateOperations, enabledWriteOperationsFilter );

            Set<Class<? extends Operation>> dependentUpdateOperationTypes = Sets.newHashSet();
            Set<Class<? extends Operation>> dependencyUpdateOperationTypes =
                    Sets.<Class<? extends Operation>>newHashSet(
                LdbcUpdate1AddPerson.class,
                            LdbcUpdate2AddPostLike.class,
                            LdbcUpdate3AddCommentLike.class,
                            LdbcUpdate4AddForum.class,
                            LdbcUpdate5AddForumMembership.class,
                            LdbcUpdate6AddPost.class,
                            LdbcUpdate7AddComment.class,
                            LdbcUpdate8AddFriendship.class
                    );

            ChildOperationGenerator updateChildOperationGenerator = null;

            ldbcSnbInteractiveWorkloadStreams.addBlockingStream(
                    dependentUpdateOperationTypes,
                    dependencyUpdateOperationTypes,
                    filteredUpdateOperations,
                    Collections.<Operation>emptyIterator(),
                    updateChildOperationGenerator
            );
        }
        return workloadStartTimeAsMilli;
    }

    /**
     * Get the operation streams (substitution parameters)
     * @param gf Generator factory to use 
     * @param workloadStartTimeAsMilli The workloadStartTimeAsMilli
     * @param loader Loader to open the csv files
     * @return List of operation streams
     * @throws WorkloadException
     */
    private List<Iterator<?>> getOperationStreams(
        GeneratorFactory gf,
        long workloadStartTimeAsMilli,
        CsvLoader loader
    ) throws WorkloadException
    {
        List<Iterator<?>> asynchronousNonDependencyStreamsList = new ArrayList<>();
         /*
         * Create read operation streams, with specified interleaves
         */

        ReadOperationStream readOperationStream = new ReadOperationStream(gf, workloadStartTimeAsMilli, loader);
        
        if ( enabledLongReadOperationTypes.contains( LdbcQuery1.class ) )
        {
            File readOperation1File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_1_PARAMS_FILENAME );
            Iterator<Operation> readOperation1Stream = readOperationStream.readOperationStream(
                Query1EventStreamReader.class,
                new Query1EventStreamReader.QueryDecoder(),
                longReadInterleavesAsMilli.get( LdbcQuery1.TYPE),
                readOperation1File
            );
            asynchronousNonDependencyStreamsList.add( readOperation1Stream );
        }

        if ( enabledLongReadOperationTypes.contains( LdbcQuery2.class ) )
        { 
            File readOperation2File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_2_PARAMS_FILENAME );
            Iterator<Operation> readOperation2Stream = readOperationStream.readOperationStream(
                Query2EventStreamReader.class,
                new Query2EventStreamReader.QueryDecoder(),
                longReadInterleavesAsMilli.get( LdbcQuery2.TYPE),
                readOperation2File
            );
            asynchronousNonDependencyStreamsList.add( readOperation2Stream ); 
        }

        if ( enabledLongReadOperationTypes.contains( LdbcQuery3.class ) )
        { 
            File readOperation3File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3_PARAMS_FILENAME );
            Iterator<Operation> readOperation3Stream = readOperationStream.readOperationStream(
                Query3EventStreamReader.class,
                new Query3EventStreamReader.QueryDecoder(),
                longReadInterleavesAsMilli.get( LdbcQuery3.TYPE),
                readOperation3File
            );
            asynchronousNonDependencyStreamsList.add( readOperation3Stream );
        }
        if ( enabledLongReadOperationTypes.contains( LdbcQuery4.class ) )
        {
            File readOperation4File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_4_PARAMS_FILENAME );
            Iterator<Operation> readOperation4Stream = readOperationStream.readOperationStream(
                Query4EventStreamReader.class,
                new Query4EventStreamReader.QueryDecoder(),
                longReadInterleavesAsMilli.get( LdbcQuery4.TYPE),
                readOperation4File
            );
            asynchronousNonDependencyStreamsList.add( readOperation4Stream ); 
        }
        if ( enabledLongReadOperationTypes.contains( LdbcQuery5.class ) )
        {
            File readOperation5File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_5_PARAMS_FILENAME );
            Iterator<Operation> readOperation5Stream = readOperationStream.readOperationStream(
                Query5EventStreamReader.class,
                new Query5EventStreamReader.QueryDecoder(),
                longReadInterleavesAsMilli.get( LdbcQuery5.TYPE),
                readOperation5File
            );
            asynchronousNonDependencyStreamsList.add( readOperation5Stream );
        }

        if ( enabledLongReadOperationTypes.contains( LdbcQuery6.class ) )
        {
            File readOperation6File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_6_PARAMS_FILENAME );
            Iterator<Operation> readOperation6Stream = readOperationStream.readOperationStream(
                Query6EventStreamReader.class,
                new Query6EventStreamReader.QueryDecoder(),
                longReadInterleavesAsMilli.get( LdbcQuery6.TYPE),
                readOperation6File
            );
            asynchronousNonDependencyStreamsList.add( readOperation6Stream );
        }

        if ( enabledLongReadOperationTypes.contains( LdbcQuery7.class ) )
        {
            File readOperation7File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_7_PARAMS_FILENAME );
            Iterator<Operation> readOperation7Stream = readOperationStream.readOperationStream(
                Query7EventStreamReader.class,
                new Query7EventStreamReader.QueryDecoder(),
                longReadInterleavesAsMilli.get( LdbcQuery7.TYPE),
                readOperation7File
            );
            asynchronousNonDependencyStreamsList.add( readOperation7Stream );
        }

        if ( enabledLongReadOperationTypes.contains( LdbcQuery8.class ) )
        {
            File readOperation8File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_8_PARAMS_FILENAME );
            Iterator<Operation> readOperation8Stream = readOperationStream.readOperationStream(
                Query8EventStreamReader.class,
                new Query8EventStreamReader.QueryDecoder(),
                longReadInterleavesAsMilli.get( LdbcQuery8.TYPE),
                readOperation8File
            );
            asynchronousNonDependencyStreamsList.add( readOperation8Stream );
        }

        if ( enabledLongReadOperationTypes.contains( LdbcQuery9.class ) )
        {
            File readOperation9File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_9_PARAMS_FILENAME );
            Iterator<Operation> readOperation9Stream = readOperationStream.readOperationStream(
                Query9EventStreamReader.class,
                new Query9EventStreamReader.QueryDecoder(),
                longReadInterleavesAsMilli.get( LdbcQuery9.TYPE),
                readOperation9File
            );
            asynchronousNonDependencyStreamsList.add( readOperation9Stream );
        }

        if ( enabledLongReadOperationTypes.contains( LdbcQuery10.class ) )
        {
            File readOperation10File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_10_PARAMS_FILENAME );
            Iterator<Operation> readOperation10Stream = readOperationStream.readOperationStream(
                Query10EventStreamReader.class,
                new Query10EventStreamReader.QueryDecoder(),
                longReadInterleavesAsMilli.get( LdbcQuery10.TYPE),
                readOperation10File
            );
            asynchronousNonDependencyStreamsList.add( readOperation10Stream );
        }

        if ( enabledLongReadOperationTypes.contains( LdbcQuery11.class ) )
        {
            File readOperation11File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_11_PARAMS_FILENAME );
            Iterator<Operation> readOperation11Stream = readOperationStream.readOperationStream(
                Query11EventStreamReader.class,
                new Query11EventStreamReader.QueryDecoder(),
                longReadInterleavesAsMilli.get( LdbcQuery11.TYPE),
                readOperation11File
            );
            asynchronousNonDependencyStreamsList.add( readOperation11Stream );
        }

        if ( enabledLongReadOperationTypes.contains( LdbcQuery12.class ) )
        {
            File readOperation12File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_12_PARAMS_FILENAME );
            Iterator<Operation> readOperation12Stream = readOperationStream.readOperationStream(
                Query12EventStreamReader.class,
                new Query12EventStreamReader.QueryDecoder(),
                longReadInterleavesAsMilli.get( LdbcQuery12.TYPE),
                readOperation12File
            );
            asynchronousNonDependencyStreamsList.add( readOperation12Stream ); 
        }

        if ( enabledLongReadOperationTypes.contains( LdbcQuery13.class ) )
        {
            File readOperation13File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13_PARAMS_FILENAME );
            Iterator<Operation> readOperation13Stream = readOperationStream.readOperationStream(
                Query13EventStreamReader.class,
                new Query13EventStreamReader.QueryDecoder(),
                longReadInterleavesAsMilli.get( LdbcQuery13.TYPE),
                readOperation13File
            );
            asynchronousNonDependencyStreamsList.add( readOperation13Stream );
        }

        if ( enabledLongReadOperationTypes.contains( LdbcQuery14.class ) )
        {
            File readOperation14File =
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14_PARAMS_FILENAME );
            Iterator<Operation> readOperation14Stream = readOperationStream.readOperationStream(
                Query14EventStreamReader.class,
                new Query14EventStreamReader.QueryDecoder(),
                longReadInterleavesAsMilli.get( LdbcQuery14.TYPE),
                readOperation14File
            );
            asynchronousNonDependencyStreamsList.add( readOperation14Stream ); 
        }

        return asynchronousNonDependencyStreamsList;
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
