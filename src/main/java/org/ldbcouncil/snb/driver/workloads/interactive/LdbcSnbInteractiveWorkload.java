package org.ldbcouncil.snb.driver.workloads.interactive;

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
import org.ldbcouncil.snb.driver.csv.ParquetLoader;
import org.ldbcouncil.snb.driver.csv.DuckDbConnectionState;
import org.ldbcouncil.snb.driver.generator.GeneratorFactory;
import org.ldbcouncil.snb.driver.generator.OperationStreamBuffer;
import org.ldbcouncil.snb.driver.generator.RandomDataGeneratorFactory;
import org.ldbcouncil.snb.driver.generator.BufferedIterator;
import org.ldbcouncil.snb.driver.generator.EventStreamReader;
import org.ldbcouncil.snb.driver.util.ClassLoaderHelper;
import org.ldbcouncil.snb.driver.util.ClassLoadingException;
import org.ldbcouncil.snb.driver.util.MapUtils;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class LdbcSnbInteractiveWorkload extends Workload
{
    private Map<Integer,Long> longReadInterleavesAsMilli;
    private File parametersDir;
    private File updatesDir;
    private long updateInterleaveAsMilli;
    private double compressionRatio;
    private double shortReadDissipationFactor;
    private OperationMode operationMode;
    private long batchSize;
    private Set<Class> enabledLongReadOperationTypes;
    private Set<Class> enabledShortReadOperationTypes;
    private Set<Class> enabledWriteOperationTypes;
    private Set<Class> enabledDeleteOperationTypes;
    private Set<Class> enabledUpdateOperationTypes;

    private RunnableOperationStreamBatchLoader runnableBatchLoader;

    @Override
    public Map<Integer, Class<? extends Operation>> operationTypeToClassMapping()
    {
        return LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping();
    }

    @Override
    public void onClose()
    {
        if (runnableBatchLoader != null && !runnableBatchLoader.isInterrupted()){
            runnableBatchLoader.interrupt();
        }
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

        if (params.containsKey(LdbcSnbInteractiveWorkloadConfiguration.BATCH_SIZE)){
            batchSize = Long.parseLong(params.get(LdbcSnbInteractiveWorkloadConfiguration.BATCH_SIZE));
        }
        else
        {
            batchSize = LdbcSnbInteractiveWorkloadConfiguration.DEFAULT_BATCH_SIZE;
        }

        compulsoryKeys.addAll( LdbcSnbInteractiveWorkloadConfiguration.LONG_READ_OPERATION_ENABLE_KEYS );
        compulsoryKeys.addAll( LdbcSnbInteractiveWorkloadConfiguration.WRITE_OPERATION_ENABLE_KEYS );
        compulsoryKeys.addAll( LdbcSnbInteractiveWorkloadConfiguration.DELETE_OPERATION_ENABLE_KEYS );
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
            updatesDir = new File( params.get( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY ).trim() );
            if ( !updatesDir.exists() )
            {
                throw new WorkloadException(
                    format( "Updates directory does not exist%nDirectory: %s",
                            updatesDir.getAbsolutePath() ) );
            }
            if ( !updatesDir.isDirectory() )
            {
                throw new WorkloadException( 
                    format( "Updates directory is not a directory%nDirectory: %s",
                        updatesDir.getAbsolutePath() ) );
            }
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

        enabledLongReadOperationTypes = getEnabledOperationsHashset(LdbcSnbInteractiveWorkloadConfiguration.LONG_READ_OPERATION_ENABLE_KEYS, params);
        enabledShortReadOperationTypes = getEnabledOperationsHashset(LdbcSnbInteractiveWorkloadConfiguration.SHORT_READ_OPERATION_ENABLE_KEYS, params);
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
                        LdbcSnbInteractiveWorkloadConfiguration.SHORT_READ_DISSIPATION , shortReadDissipationFactor ) );
            }
        }

        enabledWriteOperationTypes = getEnabledOperationsHashset(LdbcSnbInteractiveWorkloadConfiguration.WRITE_OPERATION_ENABLE_KEYS, params);
        enabledDeleteOperationTypes = getEnabledOperationsHashset(LdbcSnbInteractiveWorkloadConfiguration.DELETE_OPERATION_ENABLE_KEYS, params);
        enabledUpdateOperationTypes = new HashSet<Class>(enabledWriteOperationTypes);

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
            for (String operationFrequencyKey : LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_FREQUENCY_KEYS) {
                freqs.put(operationFrequencyKey, "1");
            }
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

        if ( enabledUpdateOperationTypes.isEmpty() &&
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
            longReadInterleavesAsMilli.put( LdbcQuery3a.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3a_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery3b.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3b_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery4.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_4_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery5.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_5_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery6.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_6_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery7.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_7_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery8.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_8_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery9.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_9_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery10.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_10_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery11.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_11_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery12.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_12_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery13a.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13a_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery13b.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13b_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery14a.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14a_INTERLEAVE_KEY ).trim() ) );
            longReadInterleavesAsMilli.put( LdbcQuery14b.TYPE, Long.parseLong(params.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14b_INTERLEAVE_KEY ).trim() ) );

        }
        catch ( NumberFormatException e )
        {
            throw new WorkloadException( "Unable to parse one of the read operation interleave values", e );
        }

        this.compressionRatio = Double.parseDouble(
                params.get( ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG ).trim()
        );
    }

    /**
     * Create set with enabled operation keys
     * @param enabledOperationKeys
     * @param params
     * @return
     */
    private Set<Class> getEnabledOperationsHashset(List<String> enabledOperationKeys, Map<String,String> params) throws WorkloadException
    {
        Set<Class> enabledOperationTypes = new HashSet<>();
        for ( String operationEnableKey : enabledOperationKeys )
        {
            String operationEnabledString = params.get( operationEnableKey ).trim();
            Boolean operationEnabled = Boolean.parseBoolean( operationEnabledString );
            String operationClassName =
                    LdbcSnbInteractiveWorkloadConfiguration.LDBC_INTERACTIVE_PACKAGE_PREFIX +
                    LdbcSnbInteractiveWorkloadConfiguration.removePrefix(
                            LdbcSnbInteractiveWorkloadConfiguration.removeSuffix(
                                operationEnableKey,
                                    LdbcSnbInteractiveWorkloadConfiguration.ENABLE_SUFFIX
                            ),
                            LdbcSnbInteractiveWorkloadConfiguration
                                    .LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX
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
                    format(
                        "Unable to load operation class for parameter: %s%nGuessed incorrect class name: %s",
                        operationEnableKey, operationClassName ),
                    e
                );
            }
        }
        return enabledOperationTypes;
    }

    /**
     * Peek the first operation and fetch the operation start time.
     * @param updateStream The Iterator with update operations
     * @param workloadStartTimeAsMilli The initial start time as milli
     * @return New workload start time as milli
     * @throws WorkloadException
     */
    private long getOperationStreamStartTime(
        Iterator<Operation> updateStream,
        long workloadStartTimeAsMilli
    ) throws WorkloadException
    {
        PeekingIterator<Operation> unfilteredUpdateOperations = Iterators.peekingIterator( updateStream );
        try
        {
            if ( unfilteredUpdateOperations.hasNext() && unfilteredUpdateOperations.peek().scheduledStartTimeAsMilli() < workloadStartTimeAsMilli )
            {
                workloadStartTimeAsMilli = unfilteredUpdateOperations.peek().scheduledStartTimeAsMilli();
            }
        }
        catch ( NoSuchElementException e )
        {
            // do nothing, exception just means that stream was empty
        }
        return workloadStartTimeAsMilli;
    }

    /**
     * Initializes the workloadstreams
     * @param gf: Generator factory with generator functions to merge iterators, create looping iterators
     * @param hasDbConnected: Whether there is a dabatase connected (used for shortreads)
     * @return Initiliazed WorkloadStreams
     * @throws WorkloadException
     */
    @Override
    protected WorkloadStreams getStreams( GeneratorFactory gf, boolean hasDbConnected ) throws WorkloadException
    {
        long workloadStartTimeAsMilli = Long.MAX_VALUE;
        WorkloadStreams ldbcSnbInteractiveWorkloadStreams = new WorkloadStreams();
        Iterator<Operation> asynchronousDependencyStreams;
        List<Iterator<?>> asynchronousNonDependencyStreamsList;
        Set<Class<? extends Operation>> dependentAsynchronousOperationTypes = Sets.newHashSet();
        Set<Class<? extends Operation>> dependencyAsynchronousOperationTypes = Sets.newHashSet();

        ParquetLoader loader;
        try {
            DuckDbConnectionState db = new DuckDbConnectionState();
            loader = new ParquetLoader(db);
        }
        catch (SQLException e){
            throw new WorkloadException(format("Error creating loader for operation streams %s", e));
        }

        ParquetLoader updateLoader;
        try {
            DuckDbConnectionState db = new DuckDbConnectionState();
            updateLoader = new ParquetLoader(db);
        }
        catch (SQLException e){
            throw new WorkloadException(format("Error creating updateLoader for operation streams %s", e));
        }


        /* 
         * WRITES
         */
        if (!enabledUpdateOperationTypes.isEmpty())
        {
            asynchronousDependencyStreams = setBatchedUpdateStreams(gf, workloadStartTimeAsMilli, updateLoader);
            workloadStartTimeAsMilli = getOperationStreamStartTime(asynchronousDependencyStreams, workloadStartTimeAsMilli);
        }
        else
        {
            asynchronousDependencyStreams = Collections.emptyIterator();
        }

        if ( Long.MAX_VALUE == workloadStartTimeAsMilli )
        {
            workloadStartTimeAsMilli = 0;
        }

        /* 
         * LONG READS
         */
        asynchronousNonDependencyStreamsList = getOperationStreams(gf, workloadStartTimeAsMilli, loader);

        /*
         * Merge all non dependency asynchronous operation streams, ordered by operation start times
         */
        Iterator<Operation> asynchronousNonDependencyStreams = gf.mergeSortOperationsByTimeStamp(
            asynchronousNonDependencyStreamsList.toArray(
                new Iterator[asynchronousNonDependencyStreamsList.size()]
            )
        );

        /* 
         * SHORT READS
         */
        ChildOperationGenerator shortReadsChildGenerator = null;
        if ( !enabledShortReadOperationTypes.isEmpty() )
        {
            shortReadsChildGenerator = getShortReadGenerator(hasDbConnected);
        }

        /* 
         * FINAL STREAMS
         */
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
     * @param hasDbConnected: Whether a database is connected
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
        ParquetLoader loader
    ) throws WorkloadException
    {
        List<Iterator<?>> asynchronousNonDependencyStreamsList = new ArrayList<>();
        /*
         * Create read operation streams, with specified interleaves
         */
        OperationStreamReader readOperationStream = new OperationStreamReader(loader);
        Map<Integer, EventStreamReader.EventDecoder<Operation>> decoders = QueryEventStreamReader.getDecoders();
        Map<Class<? extends Operation>, Integer> classToTypeMap = MapUtils.invertMap(operationTypeToClassMapping());
        for (Class enabledClass : enabledLongReadOperationTypes) {
            Integer type = classToTypeMap.get( enabledClass );
            Iterator<Operation> eventOperationStream = readOperationStream.readOperationStream(
                decoders.get(type),
                new File( parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_PARAMS_FILENAMES.get( type ))
            );
            long readOperationInterleaveAsMilli = longReadInterleavesAsMilli.get( type );
            Iterator<Long> operationStartTimes =
            gf.incrementing( workloadStartTimeAsMilli + readOperationInterleaveAsMilli,
                    readOperationInterleaveAsMilli );

            Iterator<Operation> operationStream = gf.assignStartTimes(
                operationStartTimes,
                new QueryEventStreamReader(gf.repeating( eventOperationStream ))
            );
            asynchronousNonDependencyStreamsList.add( operationStream );
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
        Integer operationTypeCount = enabledLongReadOperationTypes.size() + enabledUpdateOperationTypes.size();

        // Calculate amount of validation operations to create
        long minimumResultCountPerOperationType = Math.max(
                1,
                Math.round( Math.floor(
                        requiredValidationParameterCount.doubleValue() / operationTypeCount.doubleValue() ) )
        );

        final Map<Class,Long> remainingRequiredResultsPerType = new HashMap<>();
        for ( Class updateOperationType : enabledUpdateOperationTypes )
        {
            remainingRequiredResultsPerType.put( updateOperationType, minimumResultCountPerOperationType );
        }
        for ( Class longReadOperationType : enabledLongReadOperationTypes )
        {
            remainingRequiredResultsPerType.put( longReadOperationType, minimumResultCountPerOperationType );
        }

        for ( Class shortReadOperationType : enabledShortReadOperationTypes )
        {
            remainingRequiredResultsPerType.put( shortReadOperationType, minimumResultCountPerOperationType );
        }

        return new LdbcSnbInteractiveDbValidationParametersFilter(
            remainingRequiredResultsPerType,
            // Writes are required to determine short reads operations to inject
            enabledShortReadOperationTypes
        );
    }

    /**
     * Gets the operation class object used for serialization.
     * @return LdbcOperation
     */
    @Override
    public Class<? extends Operation> getOperationClass()
    {
        return LdbcOperation.class;
    }

    @Override
    public Set<Class> enabledValidationOperations()
    {
        Set<Class> enabledOperations = new HashSet<>();
        enabledOperations.addAll(enabledLongReadOperationTypes);
        enabledOperations.addAll(enabledUpdateOperationTypes);
        enabledOperations.addAll(enabledShortReadOperationTypes);
        return enabledOperations;
    }

    @Override
    public long maxExpectedInterleaveAsMilli()
    {
        return TimeUnit.HOURS.toMillis( 1 );
    }

    private Iterator<Operation> setBatchedUpdateStreams(
        GeneratorFactory gf,
        long workloadStartTimeAsMilli,
        ParquetLoader loader
    ) throws WorkloadException
    {
        long batchSizeInMillis = TimeUnit.HOURS.toMillis( batchSize );

        Set<Class<? extends Operation>> dependencyUpdateOperationTypes = Sets.<Class<? extends Operation>>newHashSet();

        for (Class class1 : enabledUpdateOperationTypes) {
            dependencyUpdateOperationTypes.add(class1);
        }

        int batchQueueSize = 1;
        
        BlockingQueue<Iterator<Operation>> blockingQueue = new LinkedBlockingQueue<>( batchQueueSize );
        runnableBatchLoader = new RunnableOperationStreamBatchLoader(
            loader,
            gf,
            updatesDir,
            blockingQueue,
            dependencyUpdateOperationTypes,
            batchSizeInMillis
        );
        runnableBatchLoader.start();

        OperationStreamBuffer buffer = new OperationStreamBuffer(blockingQueue);

        return new BufferedIterator(buffer);
    }
}
