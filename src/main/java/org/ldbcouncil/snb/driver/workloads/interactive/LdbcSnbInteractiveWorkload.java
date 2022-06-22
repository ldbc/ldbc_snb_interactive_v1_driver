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
import org.ldbcouncil.snb.driver.generator.GeneratorFactory;
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
    private List<File> forumUpdateOperationFiles = new ArrayList<>();
    private List<Closeable> updateOperationsFileReaders = new ArrayList<>();
    private List<File> personUpdateOperationFiles = new ArrayList<>();

    private List<Closeable> readOperationFileReaders = new ArrayList<>();
    private Map<Integer,Long> longReadInterleavesAsMilli;
    private File parametersDir;
    private long updateInterleaveAsMilli;
    private double compressionRatio;
    private double shortReadDissipationFactor;
    private OperationMode operationMode;
    private int numThreads;
    private Set<Class> enabledLongReadOperationTypes;
    private Set<Class> enabledShortReadOperationTypes;
    private Set<Class> enabledWriteOperationTypes;

    @Override
    public Map<Integer, Class<? extends Operation>> operationTypeToClassMapping()
    {
        return LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping();
    }

    @Override
    public void onInit( Map<String,String> params ) throws WorkloadException
    {
        List<String> compulsoryKeys = Lists.newArrayList();

        // Check operation mode, default is execute_benchmark
        if (params.containsKey(ConsoleAndFileDriverConfiguration.MODE_ARG)){
            operationMode = OperationMode.valueOf(params.get(ConsoleAndFileDriverConfiguration.MODE_ARG));
        }

        // Validation mode does not require parameter directory
        if (operationMode != OperationMode.validate_database )
        {
            compulsoryKeys.add( LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY );
        }

        if (params.containsKey(ConsoleAndFileDriverConfiguration.THREADS_ARG)){
            numThreads = Integer.parseInt(params.get(ConsoleAndFileDriverConfiguration.THREADS_ARG));
        }
        else
        {
            numThreads = Integer.parseInt(params.get(ConsoleAndFileDriverConfiguration.THREADS_DEFAULT_STRING));
        }

        compulsoryKeys.addAll( LdbcSnbInteractiveWorkloadConfiguration.LONG_READ_OPERATION_ENABLE_KEYS );
        compulsoryKeys.addAll( LdbcSnbInteractiveWorkloadConfiguration.WRITE_OPERATION_ENABLE_KEYS );
        compulsoryKeys.addAll( LdbcSnbInteractiveWorkloadConfiguration.SHORT_READ_OPERATION_ENABLE_KEYS );

        Set<String> missingPropertyParameters =
                LdbcSnbInteractiveWorkloadConfiguration.missingParameters( params, compulsoryKeys );
        if ( !missingPropertyParameters.isEmpty() )
        {
            throw new WorkloadException( format( "Workload could not initialize due to missing parameters: %s",
                    missingPropertyParameters.toString() ) );
        }

        if ( params.containsKey( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY ) )
        {
            String updatesDirectoryPath =
                    params.get( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY ).trim();
            File updatesDirectory = new File( updatesDirectoryPath );
            if ( !updatesDirectory.exists() )
            {
                throw new WorkloadException( format( "Updates directory does not exist\nDirectory: %s",
                        updatesDirectory.getAbsolutePath() ) );
            }
            if ( !updatesDirectory.isDirectory() )
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
            if ( !parametersDir.exists() )
            {
                throw new WorkloadException(
                        format( "Parameters directory does not exist: %s", parametersDir.getAbsolutePath() ) );
            }
            for ( String readOperationParamsFilename :
                    LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_PARAMS_FILENAMES.values() )
            {
                File readOperationParamsFile = new File( parametersDir, readOperationParamsFilename );
                if ( !readOperationParamsFile.exists() )
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
        if ( !enabledShortReadOperationTypes.isEmpty() )
        {
            if ( !params.containsKey( LdbcSnbInteractiveWorkloadConfiguration.SHORT_READ_DISSIPATION ) )
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
             !params.containsKey( LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE ) )
        {
            // if UPDATE_INTERLEAVE is missing and writes are disabled set it to DEFAULT
            params.put(
                    LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE,
                    LdbcSnbInteractiveWorkloadConfiguration.DEFAULT_UPDATE_INTERLEAVE
            );
        }
        if ( !params.containsKey( LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE ) )
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
            if ( !missingInterleaveKeys.isEmpty() )
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
    synchronized protected void onClose() throws IOException
    {
        for ( Closeable updateOperationsFileReader : updateOperationsFileReaders )
        {
            updateOperationsFileReader.close();
        }

        for ( Closeable readOperationFileReader : readOperationFileReaders )
        {
            readOperationFileReader.close();
        }
    }

    private Tuple2<Iterator<Operation>,Closeable> fileToWriteStreamParser( File updateOperationsFile ) throws IOException, WorkloadException
    {
            int bufferSize = 1 * 1024 * 1024;
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

    private Iterator<Operation> getUpdateOperationStream(File updateOperationStream) throws WorkloadException
    {
        Iterator<Operation> updateOperationsParser;
        try
        {
            Tuple2<Iterator<Operation>,Closeable> parserAndCloseable =
                    fileToWriteStreamParser( updateOperationStream );
                    updateOperationsParser = parserAndCloseable._1();
            updateOperationsFileReaders.add( parserAndCloseable._2() );
        }
        catch ( IOException e )
        {
            throw new WorkloadException(
                    "Unable to open person update stream: " + updateOperationStream.getAbsolutePath(), e );
        }
        if ( !updateOperationsParser.hasNext() )
        {
            // Update stream is empty
            throw new WorkloadException(
                    format( ""
                            + "***********************************************\n"
                            + "  !! ERROR !!\n"
                            + "  Update stream is empty: %s\n"
                            + "  Check that data generation process completed successfully\n"
                            + "***********************************************",
                            updateOperationStream.getAbsolutePath()
                    )
            );
        }
        return updateOperationsParser;
    } 

    private long filterStreamAndGetStartTime(
        Iterator<Operation> updateStream,
        ArrayList<Iterator<Operation>> operationStreamList,
        long workloadStartTimeAsMilli
    ) throws WorkloadException
    {
        PeekingIterator<Operation> unfilteredUpdateOperations = Iterators.peekingIterator( updateStream );

        try
        {
            if ( unfilteredUpdateOperations.peek().scheduledStartTimeAsMilli() <
                workloadStartTimeAsMilli )
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

        operationStreamList.add(filteredUpdateOperations);
        return workloadStartTimeAsMilli;
    }


    @Override
    protected WorkloadStreams getStreams( GeneratorFactory gf, boolean hasDbConnected ) throws WorkloadException
    {
        long workloadStartTimeAsMilli = Long.MAX_VALUE;
        WorkloadStreams ldbcSnbInteractiveWorkloadStreams = new WorkloadStreams();
        List<Iterator<?>> asynchronousDependencyStreamsList = new ArrayList<>();
        List<Iterator<?>> asynchronousNonDependencyStreamsList;// = new ArrayList<>();
        Set<Class<? extends Operation>> dependentAsynchronousOperationTypes = Sets.newHashSet();
        Set<Class<? extends Operation>> dependencyAsynchronousOperationTypes = Sets.newHashSet();

        /* *******
         * *******
         *  WRITES
         * *******/

         /*
         * Create person write operation streams
         */

        ArrayList<Iterator<Operation>> listOfOperationStreams = new ArrayList<>();

        Set<Class<? extends Operation>> dependencyUpdateOperationTypes =
        Sets.<Class<? extends Operation>>newHashSet();


        if ( enabledWriteOperationTypes.contains( LdbcUpdate1AddPerson.class ) )
        {
            for ( File personUpdateOperationFile : personUpdateOperationFiles )
            {
                dependencyUpdateOperationTypes.add(LdbcUpdate1AddPerson.class);
                Iterator<Operation> personUpdateOperationsParser= getUpdateOperationStream(personUpdateOperationFile);
                workloadStartTimeAsMilli = filterStreamAndGetStartTime(
                    personUpdateOperationsParser,
                    listOfOperationStreams,
                    workloadStartTimeAsMilli
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
                    dependencyUpdateOperationTypes.addAll(dependentForumUpdateOperationTypes);

                Iterator<Operation> forumUpdateOperationsParser = getUpdateOperationStream(forumUpdateOperationFile);
                workloadStartTimeAsMilli = filterStreamAndGetStartTime(
                    forumUpdateOperationsParser,
                    listOfOperationStreams,
                    workloadStartTimeAsMilli
                );
            }
        }
        Iterator<Operation> mergedUpdateStreams = Collections.<Operation>emptyIterator();
        for (Iterator<Operation> updateStream : listOfOperationStreams) {
            mergedUpdateStreams = gf.mergeSortOperationsByTimeStamp(mergedUpdateStreams,  updateStream);
        }

        if (numThreads == 1)
        {
            ldbcSnbInteractiveWorkloadStreams.addBlockingStream(
                Sets.newHashSet(),
                dependencyUpdateOperationTypes,
                mergedUpdateStreams,
                Collections.<Operation>emptyIterator(),
                null
            );
        }
        else{
        // Split across numThreads
            List<ArrayList<Operation>> operationLists = new ArrayList<>();
            for (int i = 0; i < numThreads; i++) {
                // Instantiate lists
                operationLists.add(new ArrayList<Operation>());
            }

            int index = 0;
            // Split accros threads
            while(mergedUpdateStreams.hasNext())
            {
                int listIndex = index % numThreads;
                Operation operation = mergedUpdateStreams.next();
                operationLists.get(listIndex).add(operation);
                index++;
            }

            // Add streams
            for (ArrayList<Operation> operationStream : operationLists) {
                
                ldbcSnbInteractiveWorkloadStreams.addBlockingStream(
                    Sets.newHashSet(),
                    dependencyUpdateOperationTypes,
                    operationStream.iterator(),
                    Collections.<Operation>emptyIterator(),
                    null
                );
            }
        }

        if ( Long.MAX_VALUE == workloadStartTimeAsMilli )
        { workloadStartTimeAsMilli = 0; }

        /* *******
         *  LONG READS
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
        
        asynchronousNonDependencyStreamsList = getOperationStreams(gf, workloadStartTimeAsMilli, loader);

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
        if ( !enabledShortReadOperationTypes.isEmpty() )
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
                asynchronousNonDependencyStreams,
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

        Queue<Long> personIdBuffer;
        Queue<Long> messageIdBuffer;
        LdbcSnbShortReadGenerator.SCHEDULED_START_TIME_POLICY scheduledStartTimePolicy;
        LdbcSnbShortReadGenerator.BufferReplenishFun bufferReplenishFun;
        if (hasDbConnected)
        {
            personIdBuffer = LdbcSnbShortReadGenerator.synchronizedCircularQueueBuffer( 1024 );
            messageIdBuffer = LdbcSnbShortReadGenerator.synchronizedCircularQueueBuffer( 1024 );
            scheduledStartTimePolicy = LdbcSnbShortReadGenerator.SCHEDULED_START_TIME_POLICY.PREVIOUS_OPERATION_ACTUAL_FINISH_TIME;
            bufferReplenishFun = new LdbcSnbShortReadGenerator.ResultBufferReplenishFun(personIdBuffer, messageIdBuffer );
        }
        else
        {
            personIdBuffer = LdbcSnbShortReadGenerator.constantBuffer( 1 );
            messageIdBuffer = LdbcSnbShortReadGenerator.constantBuffer( 1 );
            scheduledStartTimePolicy = LdbcSnbShortReadGenerator.SCHEDULED_START_TIME_POLICY.PREVIOUS_OPERATION_SCHEDULED_START_TIME;
            bufferReplenishFun = new LdbcSnbShortReadGenerator.NoOpBufferReplenishFun();
        }
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
        Map<Integer, QueryEventStreamReader.EventDecoder<Operation>> decoders = QueryEventStreamReader.getDecoders();
        Map<Class<? extends Operation>, Integer> classToTypeMap = MapUtils.invertMap(operationTypeToClassMapping());
        for (Class enabledClass : enabledLongReadOperationTypes) {
            Integer type = classToTypeMap.get( enabledClass );
            Iterator<Operation> eventOperationStream = readOperationStream.readOperationStream(
                decoders.get(type),
                longReadInterleavesAsMilli.get( type ),
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_PARAMS_FILENAMES.get( type ))
            );
            asynchronousNonDependencyStreamsList.add( eventOperationStream );
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
