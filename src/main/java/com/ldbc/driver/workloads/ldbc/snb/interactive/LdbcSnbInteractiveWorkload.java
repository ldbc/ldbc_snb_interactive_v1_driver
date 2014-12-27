package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.ldbc.driver.*;
import com.ldbc.driver.csv.*;
import com.ldbc.driver.generator.CsvEventStreamReaderBasicCharSeeker;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.ClassLoadingException;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.csv.SimpleCsvFileReader;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class LdbcSnbInteractiveWorkload extends Workload {

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

    private Set<Class> enabledReadOperationTypes;
    private Set<Class<? extends Operation<?>>> enabledWriteOperationTypes;
    private long safeTDurationAsMilli;
    private LdbcSnbInteractiveConfiguration.UpdateStreamParser parser;

    @Override
    public void onInit(Map<String, String> params) throws WorkloadException {
        List<String> compulsoryKeys = Lists.newArrayList(
                LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY);

        compulsoryKeys.addAll(LdbcSnbInteractiveConfiguration.READ_OPERATION_ENABLE_KEYS);
        compulsoryKeys.addAll(LdbcSnbInteractiveConfiguration.WRITE_OPERATION_ENABLE_KEYS);

        Set<String> missingPropertyParameters = LdbcSnbInteractiveConfiguration.missingParameters(params, compulsoryKeys);
        if (false == missingPropertyParameters.isEmpty())
            throw new WorkloadException(String.format("Workload could not initialize due to missing parameters: %s", missingPropertyParameters.toString()));

        if (params.containsKey(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY)) {
            String updatesDirectoryPath = params.get(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY);
            File updatesDirectory = new File(updatesDirectoryPath);
            if (false == updatesDirectory.exists())
                throw new WorkloadException(String.format("Updates directory does not exist\nDirectory: %s", updatesDirectory.getAbsolutePath()));
            if (false == updatesDirectory.isDirectory())
                throw new WorkloadException(String.format("Updates directory is not a directory\nDirectory: %s", updatesDirectory.getAbsolutePath()));
            forumUpdateOperationFiles = LdbcSnbInteractiveConfiguration.forumUpdateFilesInDirectory(updatesDirectory);
            personUpdateOperationFiles = LdbcSnbInteractiveConfiguration.personUpdateFilesInDirectory(updatesDirectory);
        } else {
            forumUpdateOperationFiles = new ArrayList<>();
            personUpdateOperationFiles = new ArrayList<>();
        }

        File parametersDir = new File(params.get(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY));
        if (false == parametersDir.exists()) {
            throw new WorkloadException(String.format("Parameters directory does not exist: %s", parametersDir.getAbsolutePath()));
        }
        for (String readOperationParamsFilename : LdbcSnbInteractiveConfiguration.READ_OPERATION_PARAMS_FILENAMES) {
            String readOperationParamsFullPath = parametersDir.getAbsolutePath() + "/" + readOperationParamsFilename;
            if (false == new File(readOperationParamsFullPath).exists()) {
                throw new WorkloadException(String.format("Read operation parameters file does not exist: %s", readOperationParamsFullPath));
            }
        }
        readOperation1File = new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_1_PARAMS_FILENAME);
        readOperation2File = new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_2_PARAMS_FILENAME);
        readOperation3File = new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_3_PARAMS_FILENAME);
        readOperation4File = new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_4_PARAMS_FILENAME);
        readOperation5File = new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_5_PARAMS_FILENAME);
        readOperation7File = new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_7_PARAMS_FILENAME);
        readOperation8File = new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_8_PARAMS_FILENAME);
        readOperation9File = new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_9_PARAMS_FILENAME);
        readOperation6File = new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_6_PARAMS_FILENAME);
        readOperation10File = new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_10_PARAMS_FILENAME);
        readOperation11File = new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_11_PARAMS_FILENAME);
        readOperation12File = new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_12_PARAMS_FILENAME);
        readOperation13File = new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_13_PARAMS_FILENAME);
        readOperation14File = new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_14_PARAMS_FILENAME);

        enabledReadOperationTypes = new HashSet<>();
        for (String readOperationEnableKey : LdbcSnbInteractiveConfiguration.READ_OPERATION_ENABLE_KEYS) {
            String readOperationEnabledString = params.get(readOperationEnableKey);
            Boolean readOperationEnabled = Boolean.parseBoolean(readOperationEnabledString);
            String readOperationClassName = LdbcSnbInteractiveConfiguration.LDBC_INTERACTIVE_PACKAGE_PREFIX +
                    LdbcSnbInteractiveConfiguration.removePrefix(
                            LdbcSnbInteractiveConfiguration.removeSuffix(
                                    readOperationEnableKey,
                                    LdbcSnbInteractiveConfiguration.ENABLE_SUFFIX
                            ),
                            LdbcSnbInteractiveConfiguration.LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX
                    );
            try {
                Class readOperationClass = ClassLoaderHelper.loadClass(readOperationClassName);
                if (readOperationEnabled) enabledReadOperationTypes.add(readOperationClass);
            } catch (ClassLoadingException e) {
                throw new WorkloadException(
                        String.format("Unable to load operation class for parameter: %s\nGuessed incorrect class name: %s",
                                readOperationEnableKey, readOperationClassName),
                        e
                );
            }
        }

        enabledWriteOperationTypes = new HashSet<>();
        for (String writeOperationEnableKey : LdbcSnbInteractiveConfiguration.WRITE_OPERATION_ENABLE_KEYS) {
            String writeOperationEnabledString = params.get(writeOperationEnableKey);
            Boolean writeOperationEnabled = Boolean.parseBoolean(writeOperationEnabledString);
            String writeOperationClassName = LdbcSnbInteractiveConfiguration.LDBC_INTERACTIVE_PACKAGE_PREFIX +
                    LdbcSnbInteractiveConfiguration.removePrefix(
                            LdbcSnbInteractiveConfiguration.removeSuffix(
                                    writeOperationEnableKey,
                                    LdbcSnbInteractiveConfiguration.ENABLE_SUFFIX
                            ),
                            LdbcSnbInteractiveConfiguration.LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX
                    );
            try {
                Class writeOperationClass = ClassLoaderHelper.loadClass(writeOperationClassName);
                if (writeOperationEnabled) enabledWriteOperationTypes.add(writeOperationClass);
            } catch (ClassLoadingException e) {
                throw new WorkloadException(
                        String.format("Unable to load operation class for parameter: %s\nGuessed incorrect class name: %s",
                                writeOperationEnableKey, writeOperationClassName),
                        e
                );
            }
        }

        if (false == enabledWriteOperationTypes.isEmpty()) {
            if (false == params.containsKey(LdbcSnbInteractiveConfiguration.SAFE_T)) {
                throw new WorkloadException(
                        String.format("Parameter %s must be provided when any updates are enabled", LdbcSnbInteractiveConfiguration.SAFE_T)
                );
            }
            safeTDurationAsMilli = Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.SAFE_T));
        }

        List<String> frequencyKeys = Lists.newArrayList(LdbcSnbInteractiveConfiguration.READ_OPERATION_FREQUENCY_KEYS);
        Set<String> missingFrequencyKeys = LdbcSnbInteractiveConfiguration.missingParameters(params, frequencyKeys);
        if (enabledWriteOperationTypes.isEmpty()) {
            // if UPDATE_INTERLEAVE is missing, set it to DEFAULT
            params.put(LdbcSnbInteractiveConfiguration.UPDATE_INTERLEAVE, LdbcSnbInteractiveConfiguration.DEFAULT_UPDATE_INTERLEAVE);
        }
        if (missingFrequencyKeys.isEmpty()) {
            if (false == params.containsKey(LdbcSnbInteractiveConfiguration.UPDATE_INTERLEAVE)) {
                throw new WorkloadException(String.format("Workload could not initialize. Missing parameter: %s", LdbcSnbInteractiveConfiguration.UPDATE_INTERLEAVE));
            }
            // compute interleave based on frequencies
            params = LdbcSnbInteractiveConfiguration.convertFrequenciesToInterleaves(params);
        } else {
            // if any frequencies are not set, there should be specified interleave times for read queries
            List<String> interleaveKeys = Lists.newArrayList(LdbcSnbInteractiveConfiguration.READ_OPERATION_INTERLEAVE_KEYS);
            Set<String> missingInterleaveKeys = LdbcSnbInteractiveConfiguration.missingParameters(params, interleaveKeys);
            if (false == missingInterleaveKeys.isEmpty()) {
                throw new WorkloadException(String.format("Workload could not initialize. One of the following groups of parameters should be set: %s or %s", missingFrequencyKeys.toString(), missingInterleaveKeys.toString()));
            }
        }

        try {
            readOperation1InterleaveAsMilli = Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_1_INTERLEAVE_KEY));
            readOperation2InterleaveAsMilli = Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_2_INTERLEAVE_KEY));
            readOperation3InterleaveAsMilli = Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_3_INTERLEAVE_KEY));
            readOperation4InterleaveAsMilli = Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_4_INTERLEAVE_KEY));
            readOperation5InterleaveAsMilli = Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_5_INTERLEAVE_KEY));
            readOperation6InterleaveAsMilli = Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_6_INTERLEAVE_KEY));
            readOperation7InterleaveAsMilli = Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_7_INTERLEAVE_KEY));
            readOperation8InterleaveAsMilli = Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_8_INTERLEAVE_KEY));
            readOperation9InterleaveAsMilli = Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_9_INTERLEAVE_KEY));
            readOperation10InterleaveAsMilli = Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_10_INTERLEAVE_KEY));
            readOperation11InterleaveAsMilli = Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_11_INTERLEAVE_KEY));
            readOperation12InterleaveAsMilli = Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_12_INTERLEAVE_KEY));
            readOperation13InterleaveAsMilli = Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_13_INTERLEAVE_KEY));
            readOperation14InterleaveAsMilli = Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_14_INTERLEAVE_KEY));
        } catch (NumberFormatException e) {
            throw new WorkloadException("Unable to parse one of the read operation interleave values", e);
        }

        String parserString = params.get(LdbcSnbInteractiveConfiguration.UPDATE_STREAM_PARSER);
        if (null == parserString)
            parserString = LdbcSnbInteractiveConfiguration.DEFAULT_UPDATE_STREAM_PARSER.name();
        if (false == LdbcSnbInteractiveConfiguration.isValidParser(parserString)) {
            throw new WorkloadException("Invalid parser: " + parserString);
        }
        this.parser = LdbcSnbInteractiveConfiguration.UpdateStreamParser.valueOf(parserString);
    }

    @Override
    synchronized protected void onClose() throws IOException {
        for (Closeable forumUpdateOperationsFileReader : forumUpdateOperationsFileReaders) {
            forumUpdateOperationsFileReader.close();
        }

        for (Closeable personUpdateOperationsFileReader : personUpdateOperationsFileReaders) {
            personUpdateOperationsFileReader.close();
        }

        for (Closeable readOperationFileReader : readOperationFileReaders) {
            readOperationFileReader.close();
        }
    }

    private Tuple.Tuple2<Iterator<Operation<?>>, Closeable> fileToWriteStreamParser(File updateOperationsFile, LdbcSnbInteractiveConfiguration.UpdateStreamParser parser) throws IOException, WorkloadException {
        switch (parser) {
            case REGEX: {
                SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(updateOperationsFile, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
                return Tuple.<Iterator<Operation<?>>, Closeable>tuple2(new WriteEventStreamReaderRegex(csvFileReader), csvFileReader);
            }
            case CHAR_SEEKER: {
                int bufferSize = 1 * 1024 * 1024;
                BufferedCharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new FileReader(updateOperationsFile)), bufferSize);
                Extractors extractors = new Extractors(';');
                return Tuple.<Iterator<Operation<?>>, Closeable>tuple2(new WriteEventStreamReaderCharSeeker(charSeeker, extractors, '|'), charSeeker);
            }
            case CHAR_SEEKER_THREAD: {
                int bufferSize = 1 * 1024 * 1024;
                BufferedCharSeeker charSeeker = new BufferedCharSeeker(ThreadAheadReadable.threadAhead(Readables.wrap(new FileReader(updateOperationsFile)), bufferSize), bufferSize);
                Extractors extractors = new Extractors(';');
                return Tuple.<Iterator<Operation<?>>, Closeable>tuple2(new WriteEventStreamReaderCharSeeker(charSeeker, extractors, '|'), charSeeker);
            }
        }
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(updateOperationsFile, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        return Tuple.<Iterator<Operation<?>>, Closeable>tuple2(new WriteEventStreamReaderRegex(csvFileReader), csvFileReader);
    }

    @Override
    protected WorkloadStreams getStreams(GeneratorFactory gf) throws WorkloadException {
        long workloadStartTimeAsMilli = Long.MAX_VALUE;
        WorkloadStreams ldbcSnbInteractiveWorkloadStreams = new WorkloadStreams();
        List<Iterator<?>> asynchronousDependencyStreamsList = new ArrayList<>();
        List<Iterator<?>> asynchronousNonDependencyStreamsList = new ArrayList<>();
        Set<Class<? extends Operation<?>>> dependentAsynchronousOperationTypes = Sets.newHashSet();
        Set<Class<? extends Operation<?>>> dependencyAsynchronousOperationTypes = Sets.newHashSet();

        /* *******
         * *******
         * *******
         *  WRITES
         * *******
         * *******
         * *******/

         /*
         * Create person write operation streams
         */
        for (File personUpdateOperationFile : personUpdateOperationFiles) {
            Iterator<Operation<?>> personUpdateOperationsParser;
            try {
                Tuple.Tuple2<Iterator<Operation<?>>, Closeable> parserAndCloseable = fileToWriteStreamParser(personUpdateOperationFile, parser);
                personUpdateOperationsParser = parserAndCloseable._1();
                personUpdateOperationsFileReaders.add(parserAndCloseable._2());
            } catch (IOException e) {
                throw new WorkloadException("Unable to open person update stream: " + personUpdateOperationFile.getAbsolutePath(), e);
            }
            PeekingIterator<Operation<?>> unfilteredPersonUpdateOperations = Iterators.peekingIterator(personUpdateOperationsParser);

            try {
                if (unfilteredPersonUpdateOperations.peek().scheduledStartTimeAsMilli() < workloadStartTimeAsMilli) {
                    workloadStartTimeAsMilli = unfilteredPersonUpdateOperations.peek().scheduledStartTimeAsMilli();
                }
            } catch (NoSuchElementException e) {
                // do nothing, exception just means that stream was empty
            }

            // Filter Write Operations
            Predicate<Operation<?>> enabledWriteOperationsFilter = new Predicate<Operation<?>>() {
                @Override
                public boolean apply(Operation<?> operation) {
                    return enabledWriteOperationTypes.contains(operation.getClass());
                }
            };
            Iterator<Operation<?>> filteredPersonUpdateOperations = Iterators.filter(unfilteredPersonUpdateOperations, enabledWriteOperationsFilter);

            Set<Class<? extends Operation<?>>> dependentPersonUpdateOperationTypes = Sets.newHashSet();
            Set<Class<? extends Operation<?>>> dependencyPersonUpdateOperationTypes = Sets.<Class<? extends Operation<?>>>newHashSet(
                    LdbcUpdate1AddPerson.class
            );

            ChildOperationGenerator personUpdateChildOperationGenerator = null;

            ldbcSnbInteractiveWorkloadStreams.addBlockingStream(
                    dependentPersonUpdateOperationTypes,
                    dependencyPersonUpdateOperationTypes,
                    filteredPersonUpdateOperations,
                    Collections.<Operation<?>>emptyIterator(),
                    personUpdateChildOperationGenerator
            );
        }

        /*
         * Create forum write operation streams
         */
        for (File forumUpdateOperationFile : forumUpdateOperationFiles) {
            Iterator<Operation<?>> forumUpdateOperationsParser;
            try {
                Tuple.Tuple2<Iterator<Operation<?>>, Closeable> parserAndCloseable = fileToWriteStreamParser(forumUpdateOperationFile, parser);
                forumUpdateOperationsParser = parserAndCloseable._1();
                forumUpdateOperationsFileReaders.add(parserAndCloseable._2());
            } catch (IOException e) {
                throw new WorkloadException("Unable to open forum update stream: " + forumUpdateOperationFile.getAbsolutePath(), e);
            }
            PeekingIterator<Operation<?>> unfilteredForumUpdateOperations = Iterators.peekingIterator(forumUpdateOperationsParser);

            try {
                if (unfilteredForumUpdateOperations.peek().scheduledStartTimeAsMilli() < workloadStartTimeAsMilli) {
                    workloadStartTimeAsMilli = unfilteredForumUpdateOperations.peek().scheduledStartTimeAsMilli();
                }
            } catch (NoSuchElementException e) {
                // do nothing, exception just means that stream was empty
            }

            // Filter Write Operations
            Predicate<Operation<?>> enabledWriteOperationsFilter = new Predicate<Operation<?>>() {
                @Override
                public boolean apply(Operation<?> operation) {
                    return enabledWriteOperationTypes.contains(operation.getClass());
                }
            };
            Iterator<Operation<?>> filteredForumUpdateOperations = Iterators.filter(unfilteredForumUpdateOperations, enabledWriteOperationsFilter);

            Set<Class<? extends Operation<?>>> dependentForumUpdateOperationTypes = Sets.<Class<? extends Operation<?>>>newHashSet(
                    LdbcUpdate2AddPostLike.class,
                    LdbcUpdate3AddCommentLike.class,
                    LdbcUpdate4AddForum.class,
                    LdbcUpdate5AddForumMembership.class,
                    LdbcUpdate6AddPost.class,
                    LdbcUpdate7AddComment.class,
                    LdbcUpdate8AddFriendship.class
            );
            Set<Class<? extends Operation<?>>> dependencyForumUpdateOperationTypes = Sets.newHashSet();

            ChildOperationGenerator forumUpdateChildOperationGenerator = null;

            ldbcSnbInteractiveWorkloadStreams.addBlockingStream(
                    dependentForumUpdateOperationTypes,
                    dependencyForumUpdateOperationTypes,
                    Collections.<Operation<?>>emptyIterator(),
                    filteredForumUpdateOperations,
                    forumUpdateChildOperationGenerator
            );
        }

        if (Long.MAX_VALUE == workloadStartTimeAsMilli) workloadStartTimeAsMilli = 0;

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

        Iterator<Operation<?>> readOperation1Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query1EventStreamReader.Query1Decoder();
            Extractors extractors = new Extractors(arrayDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(Readables.wrap(new FileReader(readOperation1File)), bufferSize);
            } catch (FileNotFoundException e) {
                throw new WorkloadException(String.format("Unable to open parameters file: %s", readOperation1File.getAbsolutePath()), e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(String.format("Unable to advance parameters file beyond headers: %s", readOperation1File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation1StreamWithoutTimes = new Query1EventStreamReader(
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

            Iterator<Long> operation1StartTimes = gf.incrementing(workloadStartTimeAsMilli + readOperation1InterleaveAsMilli, readOperation1InterleaveAsMilli);

            readOperation1Stream = gf.assignStartTimes(
                    operation1StartTimes,
                    operation1StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation2Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query2EventStreamReader.Query2Decoder();
            Extractors extractors = new Extractors(arrayDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(Readables.wrap(new FileReader(readOperation2File)), bufferSize);
            } catch (FileNotFoundException e) {
                throw new WorkloadException(String.format("Unable to open parameters file: %s", readOperation2File.getAbsolutePath()), e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(String.format("Unable to advance parameters file beyond headers: %s", readOperation2File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation2StreamWithoutTimes = new Query2EventStreamReader(
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

            Iterator<Long> operation2StartTimes = gf.incrementing(workloadStartTimeAsMilli + readOperation2InterleaveAsMilli, readOperation2InterleaveAsMilli);

            readOperation2Stream = gf.assignStartTimes(
                    operation2StartTimes,
                    operation2StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation3Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query3EventStreamReader.Query3Decoder();
            Extractors extractors = new Extractors(arrayDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(Readables.wrap(new FileReader(readOperation3File)), bufferSize);
            } catch (FileNotFoundException e) {
                throw new WorkloadException(String.format("Unable to open parameters file: %s", readOperation3File.getAbsolutePath()), e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(String.format("Unable to advance parameters file beyond headers: %s", readOperation3File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation3StreamWithoutTimes = new Query3EventStreamReader(
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

            Iterator<Long> operation3StartTimes = gf.incrementing(workloadStartTimeAsMilli + readOperation3InterleaveAsMilli, readOperation3InterleaveAsMilli);

            readOperation3Stream = gf.assignStartTimes(
                    operation3StartTimes,
                    operation3StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation4Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query4EventStreamReader.Query4Decoder();
            Extractors extractors = new Extractors(arrayDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(Readables.wrap(new FileReader(readOperation4File)), bufferSize);
            } catch (FileNotFoundException e) {
                throw new WorkloadException(String.format("Unable to open parameters file: %s", readOperation4File.getAbsolutePath()), e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(String.format("Unable to advance parameters file beyond headers: %s", readOperation4File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation4StreamWithoutTimes = new Query4EventStreamReader(
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

            Iterator<Long> operation4StartTimes = gf.incrementing(workloadStartTimeAsMilli + readOperation4InterleaveAsMilli, readOperation4InterleaveAsMilli);

            readOperation4Stream = gf.assignStartTimes(
                    operation4StartTimes,
                    operation4StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation5Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query5EventStreamReader.Query5Decoder();
            Extractors extractors = new Extractors(arrayDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(Readables.wrap(new FileReader(readOperation5File)), bufferSize);
            } catch (FileNotFoundException e) {
                throw new WorkloadException(String.format("Unable to open parameters file: %s", readOperation5File.getAbsolutePath()), e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(String.format("Unable to advance parameters file beyond headers: %s", readOperation5File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation5StreamWithoutTimes = new Query5EventStreamReader(
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

            Iterator<Long> operation5StartTimes = gf.incrementing(workloadStartTimeAsMilli + readOperation5InterleaveAsMilli, readOperation5InterleaveAsMilli);

            readOperation5Stream = gf.assignStartTimes(
                    operation5StartTimes,
                    operation5StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation6Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query6EventStreamReader.Query6Decoder();
            Extractors extractors = new Extractors(arrayDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(Readables.wrap(new FileReader(readOperation6File)), bufferSize);
            } catch (FileNotFoundException e) {
                throw new WorkloadException(String.format("Unable to open parameters file: %s", readOperation6File.getAbsolutePath()), e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(String.format("Unable to advance parameters file beyond headers: %s", readOperation6File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation6StreamWithoutTimes = new Query6EventStreamReader(
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

            Iterator<Long> operation6StartTimes = gf.incrementing(workloadStartTimeAsMilli + readOperation6InterleaveAsMilli, readOperation6InterleaveAsMilli);

            readOperation6Stream = gf.assignStartTimes(
                    operation6StartTimes,
                    operation6StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation7Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query7EventStreamReader.Query7Decoder();
            Extractors extractors = new Extractors(arrayDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(Readables.wrap(new FileReader(readOperation7File)), bufferSize);
            } catch (FileNotFoundException e) {
                throw new WorkloadException(String.format("Unable to open parameters file: %s", readOperation7File.getAbsolutePath()), e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(String.format("Unable to advance parameters file beyond headers: %s", readOperation7File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation7StreamWithoutTimes = new Query7EventStreamReader(
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

            Iterator<Long> operation7StartTimes = gf.incrementing(workloadStartTimeAsMilli + readOperation7InterleaveAsMilli, readOperation7InterleaveAsMilli);

            readOperation7Stream = gf.assignStartTimes(
                    operation7StartTimes,
                    operation7StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation8Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query8EventStreamReader.Query8Decoder();
            Extractors extractors = new Extractors(arrayDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(Readables.wrap(new FileReader(readOperation8File)), bufferSize);
            } catch (FileNotFoundException e) {
                throw new WorkloadException(String.format("Unable to open parameters file: %s", readOperation8File.getAbsolutePath()), e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(String.format("Unable to advance parameters file beyond headers: %s", readOperation8File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation8StreamWithoutTimes = new Query8EventStreamReader(
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

            Iterator<Long> operation8StartTimes = gf.incrementing(workloadStartTimeAsMilli + readOperation8InterleaveAsMilli, readOperation8InterleaveAsMilli);

            readOperation8Stream = gf.assignStartTimes(
                    operation8StartTimes,
                    operation8StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation9Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query9EventStreamReader.Query9Decoder();
            Extractors extractors = new Extractors(arrayDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(Readables.wrap(new FileReader(readOperation9File)), bufferSize);
            } catch (FileNotFoundException e) {
                throw new WorkloadException(String.format("Unable to open parameters file: %s", readOperation9File.getAbsolutePath()), e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(String.format("Unable to advance parameters file beyond headers: %s", readOperation9File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation9StreamWithoutTimes = new Query9EventStreamReader(
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

            Iterator<Long> operation9StartTimes = gf.incrementing(workloadStartTimeAsMilli + readOperation9InterleaveAsMilli, readOperation9InterleaveAsMilli);

            readOperation9Stream = gf.assignStartTimes(
                    operation9StartTimes,
                    operation9StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation10Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query10EventStreamReader.Query10Decoder();
            Extractors extractors = new Extractors(arrayDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(Readables.wrap(new FileReader(readOperation10File)), bufferSize);
            } catch (FileNotFoundException e) {
                throw new WorkloadException(String.format("Unable to open parameters file: %s", readOperation10File.getAbsolutePath()), e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(String.format("Unable to advance parameters file beyond headers: %s", readOperation10File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation10StreamWithoutTimes = new Query10EventStreamReader(
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

            Iterator<Long> operation10StartTimes = gf.incrementing(workloadStartTimeAsMilli + readOperation10InterleaveAsMilli, readOperation10InterleaveAsMilli);

            readOperation10Stream = gf.assignStartTimes(
                    operation10StartTimes,
                    operation10StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation11Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query11EventStreamReader.Query11Decoder();
            Extractors extractors = new Extractors(arrayDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(Readables.wrap(new FileReader(readOperation11File)), bufferSize);
            } catch (FileNotFoundException e) {
                throw new WorkloadException(String.format("Unable to open parameters file: %s", readOperation11File.getAbsolutePath()), e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(String.format("Unable to advance parameters file beyond headers: %s", readOperation11File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation11StreamWithoutTimes = new Query11EventStreamReader(
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

            Iterator<Long> operation11StartTimes = gf.incrementing(workloadStartTimeAsMilli + readOperation11InterleaveAsMilli, readOperation11InterleaveAsMilli);

            readOperation11Stream = gf.assignStartTimes(
                    operation11StartTimes,
                    operation11StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation12Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query12EventStreamReader.Query12Decoder();
            Extractors extractors = new Extractors(arrayDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(Readables.wrap(new FileReader(readOperation12File)), bufferSize);
            } catch (FileNotFoundException e) {
                throw new WorkloadException(String.format("Unable to open parameters file: %s", readOperation12File.getAbsolutePath()), e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(String.format("Unable to advance parameters file beyond headers: %s", readOperation12File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation12StreamWithoutTimes = new Query12EventStreamReader(
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

            Iterator<Long> operation12StartTimes = gf.incrementing(workloadStartTimeAsMilli + readOperation12InterleaveAsMilli, readOperation12InterleaveAsMilli);

            readOperation12Stream = gf.assignStartTimes(
                    operation12StartTimes,
                    operation12StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation13Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query13EventStreamReader.Query13Decoder();
            Extractors extractors = new Extractors(arrayDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(Readables.wrap(new FileReader(readOperation13File)), bufferSize);
            } catch (FileNotFoundException e) {
                throw new WorkloadException(String.format("Unable to open parameters file: %s", readOperation13File.getAbsolutePath()), e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(String.format("Unable to advance parameters file beyond headers: %s", readOperation13File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation13StreamWithoutTimes = new Query13EventStreamReader(
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

            Iterator<Long> operation13StartTimes = gf.incrementing(workloadStartTimeAsMilli + readOperation13InterleaveAsMilli, readOperation13InterleaveAsMilli);

            readOperation13Stream = gf.assignStartTimes(
                    operation13StartTimes,
                    operation13StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation14Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder = new Query14EventStreamReader.Query14Decoder();
            Extractors extractors = new Extractors(arrayDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(Readables.wrap(new FileReader(readOperation14File)), bufferSize);
            } catch (FileNotFoundException e) {
                throw new WorkloadException(String.format("Unable to open parameters file: %s", readOperation14File.getAbsolutePath()), e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(String.format("Unable to advance parameters file beyond headers: %s", readOperation14File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation14StreamWithoutTimes = new Query14EventStreamReader(
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

            Iterator<Long> operation14StartTimes = gf.incrementing(workloadStartTimeAsMilli + readOperation14InterleaveAsMilli, readOperation14InterleaveAsMilli);

            readOperation14Stream = gf.assignStartTimes(
                    operation14StartTimes,
                    operation14StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        if (enabledReadOperationTypes.contains(LdbcQuery1.class))
            asynchronousNonDependencyStreamsList.add(readOperation1Stream);
        if (enabledReadOperationTypes.contains(LdbcQuery2.class))
            asynchronousNonDependencyStreamsList.add(readOperation2Stream);
        if (enabledReadOperationTypes.contains(LdbcQuery3.class))
            asynchronousNonDependencyStreamsList.add(readOperation3Stream);
        if (enabledReadOperationTypes.contains(LdbcQuery4.class))
            asynchronousNonDependencyStreamsList.add(readOperation4Stream);
        if (enabledReadOperationTypes.contains(LdbcQuery5.class))
            asynchronousNonDependencyStreamsList.add(readOperation5Stream);
        if (enabledReadOperationTypes.contains(LdbcQuery6.class))
            asynchronousNonDependencyStreamsList.add(readOperation6Stream);
        if (enabledReadOperationTypes.contains(LdbcQuery7.class))
            asynchronousNonDependencyStreamsList.add(readOperation7Stream);
        if (enabledReadOperationTypes.contains(LdbcQuery8.class))
            asynchronousNonDependencyStreamsList.add(readOperation8Stream);
        if (enabledReadOperationTypes.contains(LdbcQuery9.class))
            asynchronousNonDependencyStreamsList.add(readOperation9Stream);
        if (enabledReadOperationTypes.contains(LdbcQuery10.class))
            asynchronousNonDependencyStreamsList.add(readOperation10Stream);
        if (enabledReadOperationTypes.contains(LdbcQuery11.class))
            asynchronousNonDependencyStreamsList.add(readOperation11Stream);
        if (enabledReadOperationTypes.contains(LdbcQuery12.class))
            asynchronousNonDependencyStreamsList.add(readOperation12Stream);
        if (enabledReadOperationTypes.contains(LdbcQuery13.class))
            asynchronousNonDependencyStreamsList.add(readOperation13Stream);
        if (enabledReadOperationTypes.contains(LdbcQuery14.class))
            asynchronousNonDependencyStreamsList.add(readOperation14Stream);

        /*
         * Merge all dependency asynchronous operation streams, ordered by operation start times
         */
        Iterator<Operation<?>> asynchronousDependencyStreams = gf.mergeSortOperationsByTimeStamp(
                asynchronousDependencyStreamsList.toArray(new Iterator[asynchronousDependencyStreamsList.size()])
        );
        /*
         * Merge all non dependency asynchronous operation streams, ordered by operation start times
         */
        Iterator<Operation<?>> asynchronousNonDependencyStreams = gf.mergeSortOperationsByTimeStamp(
                asynchronousNonDependencyStreamsList.toArray(new Iterator[asynchronousNonDependencyStreamsList.size()])
        );

        /* **************
         * **************
         * **************
         *  FINAL STREAMS
         * **************
         * **************
         * **************/

        // TODO tune this
        // TODO expose some tuning stuff through config
        double initialProbability = 1.0;
        double probabilityDegradationFactor = 0.5;
        double minimumProbability = 0.1;
        ChildOperationGenerator shortReadsChildGenerator = new LdbcSnbShortReadGenerator(
                initialProbability,
                probabilityDegradationFactor,
                minimumProbability
        );

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
    public DbValidationParametersFilter dbValidationParametersFilter(Integer requiredValidationParameterCount) {
        /**
         * TODO
         * operationTypeCount = 14
         * requiredValidationParameterCount = 100
         * 100/14 == 7.14
         * --> minimumResultCountPerOperationType = 7
         * 100%14 == 2 > 0
         * --> minimumResultCountPerOperationType = 8
         */
        Integer operationTypeCount = enabledReadOperationTypes.size();
        long minimumResultCountPerOperationType = Math.max(1, Math.round(Math.floor(requiredValidationParameterCount.doubleValue() / operationTypeCount.doubleValue())));

//        if (requiredValidationParameterCount % operationTypeCount > 0)
//            minimumResultCountPerOperationType++;

        final Map<Class, Long> remainingRequiredResultsPerOperationType = new HashMap<>();
        long resultCountsAssignedSoFar = 0;
        for (Class operationType : enabledReadOperationTypes) {
            remainingRequiredResultsPerOperationType.put(operationType, minimumResultCountPerOperationType);
            resultCountsAssignedSoFar = resultCountsAssignedSoFar + minimumResultCountPerOperationType;
        }
        for (Class operationType : remainingRequiredResultsPerOperationType.keySet()) {
            if (resultCountsAssignedSoFar >= requiredValidationParameterCount)
                break;
            remainingRequiredResultsPerOperationType.put(operationType, remainingRequiredResultsPerOperationType.get(operationType) + 1);
            resultCountsAssignedSoFar++;
        }

        return new DbValidationParametersFilter() {
            @Override
            public boolean useOperation(Operation<?> operation) {
                Class operationType = operation.getClass();

                boolean isNotReadOperation = false == enabledReadOperationTypes.contains(operationType);
                if (isNotReadOperation) return false;

                boolean alreadyHaveAllRequiredResultsForOperationType = false == remainingRequiredResultsPerOperationType.containsKey(operationType);
                if (alreadyHaveAllRequiredResultsForOperationType) return false;

                return true;
            }

            @Override
            public DbValidationParametersFilterResult useOperationAndResultForValidation(Operation<?> operation, Object operationResult) {
                Class operationType = operation.getClass();

                boolean isEmptyResult = ((List) operationResult).isEmpty();
                if (isEmptyResult) {
                    return DbValidationParametersFilterResult.REJECT_AND_CONTINUE;
                }

                long remainingRequiredResultsForOperationType = remainingRequiredResultsPerOperationType.get(operationType) - 1;

                if (0 == remainingRequiredResultsForOperationType)
                    remainingRequiredResultsPerOperationType.remove(operationType);
                else
                    remainingRequiredResultsPerOperationType.put(operationType, remainingRequiredResultsForOperationType);

                if (remainingRequiredResultsPerOperationType.size() > 0) {
                    return DbValidationParametersFilterResult.ACCEPT_AND_CONTINUE;
                } else {
                    return DbValidationParametersFilterResult.ACCEPT_AND_FINISH;
                }
            }
        };
    }

    @Override
    public long maxExpectedInterleaveAsMilli() {
        TemporalUtil temporalUtil = new TemporalUtil();
        return temporalUtil.convert(1, TimeUnit.HOURS, TimeUnit.MILLISECONDS);
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference TYPE_REFERENCE = new TypeReference<List<Object>>() {
    };

    @Override
    public String serializeOperation(Operation<?> operation) throws SerializingMarshallingException {
        if (operation.getClass().equals(LdbcQuery1.class)) {
            LdbcQuery1 ldbcQuery = (LdbcQuery1) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.firstName());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery2.class)) {
            LdbcQuery2 ldbcQuery = (LdbcQuery2) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.maxDate().getTime());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery3.class)) {
            LdbcQuery3 ldbcQuery = (LdbcQuery3) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.countryXName());
            operationAsList.add(ldbcQuery.countryYName());
            operationAsList.add(ldbcQuery.startDate().getTime());
            operationAsList.add(ldbcQuery.durationDays());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery4.class)) {
            LdbcQuery4 ldbcQuery = (LdbcQuery4) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.startDate().getTime());
            operationAsList.add(ldbcQuery.durationDays());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery5.class)) {
            LdbcQuery5 ldbcQuery = (LdbcQuery5) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.minDate().getTime());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery6.class)) {
            LdbcQuery6 ldbcQuery = (LdbcQuery6) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.tagName());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery7.class)) {
            LdbcQuery7 ldbcQuery = (LdbcQuery7) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery8.class)) {
            LdbcQuery8 ldbcQuery = (LdbcQuery8) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery9.class)) {
            LdbcQuery9 ldbcQuery = (LdbcQuery9) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.maxDate().getTime());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery10.class)) {
            LdbcQuery10 ldbcQuery = (LdbcQuery10) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.month());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery11.class)) {
            LdbcQuery11 ldbcQuery = (LdbcQuery11) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.countryName());
            operationAsList.add(ldbcQuery.workFromYear());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery12.class)) {
            LdbcQuery12 ldbcQuery = (LdbcQuery12) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.tagClassName());
            operationAsList.add(ldbcQuery.limit());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery13.class)) {
            LdbcQuery13 ldbcQuery = (LdbcQuery13) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.person1Id());
            operationAsList.add(ldbcQuery.person2Id());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcQuery14.class)) {
            LdbcQuery14 ldbcQuery = (LdbcQuery14) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.person1Id());
            operationAsList.add(ldbcQuery.person2Id());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcUpdate1AddPerson.class)) {
            LdbcUpdate1AddPerson ldbcQuery = (LdbcUpdate1AddPerson) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.personFirstName());
            operationAsList.add(ldbcQuery.personLastName());
            operationAsList.add(ldbcQuery.gender());
            operationAsList.add(ldbcQuery.birthday().getTime());
            operationAsList.add(ldbcQuery.creationDate().getTime());
            operationAsList.add(ldbcQuery.locationIp());
            operationAsList.add(ldbcQuery.browserUsed());
            operationAsList.add(ldbcQuery.cityId());
            operationAsList.add(ldbcQuery.languages());
            operationAsList.add(ldbcQuery.emails());
            operationAsList.add(ldbcQuery.tagIds());
            Iterable<Map<String, Object>> studyAt = Lists.newArrayList(Iterables.transform(ldbcQuery.studyAt(), new Function<LdbcUpdate1AddPerson.Organization, Map<String, Object>>() {
                @Override
                public Map<String, Object> apply(LdbcUpdate1AddPerson.Organization organization) {
                    Map<String, Object> organizationMap = new HashMap<>();
                    organizationMap.put("id", organization.organizationId());
                    organizationMap.put("year", organization.year());
                    return organizationMap;
                }
            }));
            operationAsList.add(studyAt);
            Iterable<Map<String, Object>> workAt = Lists.newArrayList(Iterables.transform(ldbcQuery.workAt(), new Function<LdbcUpdate1AddPerson.Organization, Map<String, Object>>() {
                @Override
                public Map<String, Object> apply(LdbcUpdate1AddPerson.Organization organization) {
                    Map<String, Object> organizationMap = new HashMap<>();
                    organizationMap.put("id", organization.organizationId());
                    organizationMap.put("year", organization.year());
                    return organizationMap;
                }
            }));
            operationAsList.add(workAt);
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcUpdate2AddPostLike.class)) {
            LdbcUpdate2AddPostLike ldbcQuery = (LdbcUpdate2AddPostLike) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.postId());
            operationAsList.add(ldbcQuery.creationDate().getTime());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcUpdate3AddCommentLike.class)) {
            LdbcUpdate3AddCommentLike ldbcQuery = (LdbcUpdate3AddCommentLike) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.commentId());
            operationAsList.add(ldbcQuery.creationDate().getTime());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcUpdate4AddForum.class)) {
            LdbcUpdate4AddForum ldbcQuery = (LdbcUpdate4AddForum) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.forumId());
            operationAsList.add(ldbcQuery.forumTitle());
            operationAsList.add(ldbcQuery.creationDate().getTime());
            operationAsList.add(ldbcQuery.moderatorPersonId());
            operationAsList.add(ldbcQuery.tagIds());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcUpdate5AddForumMembership.class)) {
            LdbcUpdate5AddForumMembership ldbcQuery = (LdbcUpdate5AddForumMembership) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.forumId());
            operationAsList.add(ldbcQuery.personId());
            operationAsList.add(ldbcQuery.joinDate().getTime());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcUpdate6AddPost.class)) {
            LdbcUpdate6AddPost ldbcQuery = (LdbcUpdate6AddPost) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.postId());
            operationAsList.add(ldbcQuery.imageFile());
            operationAsList.add(ldbcQuery.creationDate().getTime());
            operationAsList.add(ldbcQuery.locationIp());
            operationAsList.add(ldbcQuery.browserUsed());
            operationAsList.add(ldbcQuery.language());
            operationAsList.add(ldbcQuery.content());
            operationAsList.add(ldbcQuery.length());
            operationAsList.add(ldbcQuery.authorPersonId());
            operationAsList.add(ldbcQuery.forumId());
            operationAsList.add(ldbcQuery.countryId());
            operationAsList.add(ldbcQuery.tagIds());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcUpdate7AddComment.class)) {
            LdbcUpdate7AddComment ldbcQuery = (LdbcUpdate7AddComment) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.commentId());
            operationAsList.add(ldbcQuery.creationDate());
            operationAsList.add(ldbcQuery.locationIp());
            operationAsList.add(ldbcQuery.browserUsed());
            operationAsList.add(ldbcQuery.content());
            operationAsList.add(ldbcQuery.length());
            operationAsList.add(ldbcQuery.authorPersonId());
            operationAsList.add(ldbcQuery.countryId());
            operationAsList.add(ldbcQuery.replyToPostId());
            operationAsList.add(ldbcQuery.replyToCommentId());
            operationAsList.add(ldbcQuery.tagIds());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        if (operation.getClass().equals(LdbcUpdate8AddFriendship.class)) {
            LdbcUpdate8AddFriendship ldbcQuery = (LdbcUpdate8AddFriendship) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add(ldbcQuery.getClass().getName());
            operationAsList.add(ldbcQuery.person1Id());
            operationAsList.add(ldbcQuery.person2Id());
            operationAsList.add(ldbcQuery.creationDate().getTime());
            try {
                return OBJECT_MAPPER.writeValueAsString(operationAsList);
            } catch (IOException e) {
                throw new SerializingMarshallingException(String.format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
            }
        }

        throw new SerializingMarshallingException(
                String.format("Workload does not know how to serialize operation\nWorkload: %s\nOperation Type: %s\nOperation: %s",
                        getClass().getName(),
                        operation.getClass().getName(),
                        operation));
    }

    @Override
    public Operation<?> marshalOperation(String serializedOperation) throws SerializingMarshallingException {
        List<Object> operationAsList;
        try {
            operationAsList = OBJECT_MAPPER.readValue(serializedOperation, TYPE_REFERENCE);
        } catch (IOException e) {
            throw new SerializingMarshallingException(String.format("Error while parsing serialized results\n%s", serializedOperation), e);
        }

        if (operationAsList.get(0).equals(LdbcQuery1.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String firstName = (String) operationAsList.get(2);
            int limit = ((Number) operationAsList.get(3)).intValue();
            return new LdbcQuery1(personId, firstName, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery2.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            Date maxDate = new Date(((Number) operationAsList.get(2)).longValue());
            int limit = ((Number) operationAsList.get(3)).intValue();
            return new LdbcQuery2(personId, maxDate, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery3.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String countryXName = (String) operationAsList.get(2);
            String countryYName = (String) operationAsList.get(3);
            Date startDate = new Date(((Number) operationAsList.get(4)).longValue());
            int durationDays = ((Number) operationAsList.get(5)).intValue();
            int limit = ((Number) operationAsList.get(6)).intValue();
            return new LdbcQuery3(personId, countryXName, countryYName, startDate, durationDays, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery4.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            Date startDate = new Date(((Number) operationAsList.get(2)).longValue());
            int durationDays = ((Number) operationAsList.get(3)).intValue();
            int limit = ((Number) operationAsList.get(4)).intValue();
            return new LdbcQuery4(personId, startDate, durationDays, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery5.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            Date minDate = new Date(((Number) operationAsList.get(2)).longValue());
            int limit = ((Number) operationAsList.get(3)).intValue();
            return new LdbcQuery5(personId, minDate, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery6.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String tagName = (String) operationAsList.get(2);
            int limit = ((Number) operationAsList.get(3)).intValue();
            return new LdbcQuery6(personId, tagName, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery7.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            int limit = ((Number) operationAsList.get(2)).intValue();
            return new LdbcQuery7(personId, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery8.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            int limit = ((Number) operationAsList.get(2)).intValue();
            return new LdbcQuery8(personId, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery9.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            Date maxDate = new Date(((Number) operationAsList.get(2)).longValue());
            int limit = ((Number) operationAsList.get(3)).intValue();
            return new LdbcQuery9(personId, maxDate, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery10.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            int month = ((Number) operationAsList.get(2)).intValue();
            int limit = ((Number) operationAsList.get(3)).intValue();
            return new LdbcQuery10(personId, month, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery11.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String countryName = (String) operationAsList.get(2);
            int workFromYear = ((Number) operationAsList.get(3)).intValue();
            int limit = ((Number) operationAsList.get(4)).intValue();
            return new LdbcQuery11(personId, countryName, workFromYear, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery12.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String tagClassName = (String) operationAsList.get(2);
            int limit = ((Number) operationAsList.get(3)).intValue();
            return new LdbcQuery12(personId, tagClassName, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery13.class.getName())) {
            long person1Id = ((Number) operationAsList.get(1)).longValue();
            long person2Id = ((Number) operationAsList.get(2)).longValue();
            return new LdbcQuery13(person1Id, person2Id);
        }

        if (operationAsList.get(0).equals(LdbcQuery14.class.getName())) {
            long person1Id = ((Number) operationAsList.get(1)).longValue();
            long person2Id = ((Number) operationAsList.get(2)).longValue();
            return new LdbcQuery14(person1Id, person2Id);
        }

        if (operationAsList.get(0).equals(LdbcUpdate1AddPerson.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personFirstName = (String) operationAsList.get(2);
            String personLastName = (String) operationAsList.get(3);
            String gender = (String) operationAsList.get(4);
            Date birthday = new Date(((Number) operationAsList.get(5)).longValue());
            Date creationDate = new Date(((Number) operationAsList.get(6)).longValue());
            String locationIp = (String) operationAsList.get(7);
            String browserUsed = (String) operationAsList.get(8);
            long cityId = ((Number) operationAsList.get(9)).longValue();
            List<String> languages = (List<String>) operationAsList.get(10);
            List<String> emails = (List<String>) operationAsList.get(11);
            List<Long> tagIds = Lists.newArrayList(Iterables.transform((List<Number>) operationAsList.get(12), new Function<Number, Long>() {
                @Override
                public Long apply(Number number) {
                    return number.longValue();
                }
            }));
            List<Map<String, Object>> studyAtList = (List<Map<String, Object>>) operationAsList.get(13);
            List<LdbcUpdate1AddPerson.Organization> studyAt = Lists.newArrayList(Iterables.transform(studyAtList, new Function<Map<String, Object>, LdbcUpdate1AddPerson.Organization>() {
                @Override
                public LdbcUpdate1AddPerson.Organization apply(Map<String, Object> input) {
                    long organizationId = ((Number) input.get("id")).longValue();
                    int year = ((Number) input.get("year")).intValue();
                    return new LdbcUpdate1AddPerson.Organization(organizationId, year);
                }
            }));
            List<Map<String, Object>> workAtList = (List<Map<String, Object>>) operationAsList.get(14);
            List<LdbcUpdate1AddPerson.Organization> workAt = Lists.newArrayList(Iterables.transform(workAtList, new Function<Map<String, Object>, LdbcUpdate1AddPerson.Organization>() {
                @Override
                public LdbcUpdate1AddPerson.Organization apply(Map<String, Object> input) {
                    long organizationId = ((Number) input.get("id")).longValue();
                    int year = ((Number) input.get("year")).intValue();
                    return new LdbcUpdate1AddPerson.Organization(organizationId, year);
                }
            }));

            return new LdbcUpdate1AddPerson(personId, personFirstName, personLastName, gender, birthday, creationDate,
                    locationIp, browserUsed, cityId, languages, emails, tagIds, studyAt, workAt);
        }

        if (operationAsList.get(0).equals(LdbcUpdate2AddPostLike.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            long postId = ((Number) operationAsList.get(2)).longValue();
            Date creationDate = new Date(((Number) operationAsList.get(3)).longValue());

            return new LdbcUpdate2AddPostLike(personId, postId, creationDate);
        }

        if (operationAsList.get(0).equals(LdbcUpdate3AddCommentLike.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            long commentId = ((Number) operationAsList.get(2)).longValue();
            Date creationDate = new Date(((Number) operationAsList.get(3)).longValue());

            return new LdbcUpdate3AddCommentLike(personId, commentId, creationDate);
        }

        if (operationAsList.get(0).equals(LdbcUpdate4AddForum.class.getName())) {
            long forumId = ((Number) operationAsList.get(1)).longValue();
            String forumTitle = (String) operationAsList.get(2);
            Date creationDate = new Date(((Number) operationAsList.get(3)).longValue());
            long moderatorPersonId = ((Number) operationAsList.get(4)).longValue();
            List<Long> tagIds = Lists.newArrayList(Iterables.transform((List<Number>) operationAsList.get(5), new Function<Number, Long>() {
                @Override
                public Long apply(Number number) {
                    return number.longValue();
                }
            }));

            return new LdbcUpdate4AddForum(forumId, forumTitle, creationDate, moderatorPersonId, tagIds);
        }


        if (operationAsList.get(0).equals(LdbcUpdate5AddForumMembership.class.getName())) {
            long forumId = ((Number) operationAsList.get(1)).longValue();
            long personId = ((Number) operationAsList.get(2)).longValue();
            Date creationDate = new Date(((Number) operationAsList.get(3)).longValue());

            return new LdbcUpdate5AddForumMembership(forumId, personId, creationDate);
        }

        if (operationAsList.get(0).equals(LdbcUpdate6AddPost.class.getName())) {
            long postId = ((Number) operationAsList.get(1)).longValue();
            String imageFile = (String) operationAsList.get(2);
            Date creationDate = new Date(((Number) operationAsList.get(3)).longValue());
            String locationIp = (String) operationAsList.get(4);
            String browserUsed = (String) operationAsList.get(5);
            String language = (String) operationAsList.get(6);
            String content = (String) operationAsList.get(7);
            int length = ((Number) operationAsList.get(8)).intValue();
            long authorPersonId = ((Number) operationAsList.get(9)).longValue();
            long forumId = ((Number) operationAsList.get(10)).longValue();
            long countryId = ((Number) operationAsList.get(11)).longValue();
            List<Long> tagIds = Lists.newArrayList(Iterables.transform((List<Number>) operationAsList.get(12), new Function<Number, Long>() {
                @Override
                public Long apply(Number number) {
                    return number.longValue();
                }
            }));

            return new LdbcUpdate6AddPost(postId, imageFile, creationDate, locationIp, browserUsed, language, content, length, authorPersonId, forumId, countryId, tagIds);
        }

        if (operationAsList.get(0).equals(LdbcUpdate7AddComment.class.getName())) {
            long commentId = ((Number) operationAsList.get(1)).longValue();
            Date creationDate = new Date(((Number) operationAsList.get(2)).longValue());
            String locationIp = (String) operationAsList.get(3);
            String browserUsed = (String) operationAsList.get(4);
            String content = (String) operationAsList.get(5);
            int length = ((Number) operationAsList.get(6)).intValue();
            long authorPersonId = ((Number) operationAsList.get(7)).longValue();
            long countryId = ((Number) operationAsList.get(8)).longValue();
            long replyToPostId = ((Number) operationAsList.get(9)).longValue();
            long replyToCommentId = ((Number) operationAsList.get(10)).longValue();
            List<Long> tagIds = Lists.newArrayList(Iterables.transform((List<Number>) operationAsList.get(11), new Function<Number, Long>() {
                @Override
                public Long apply(Number number) {
                    return number.longValue();
                }
            }));

            return new LdbcUpdate7AddComment(commentId, creationDate, locationIp, browserUsed, content, length, authorPersonId, countryId, replyToPostId, replyToCommentId, tagIds);
        }

        if (operationAsList.get(0).equals(LdbcUpdate8AddFriendship.class.getName())) {
            long person1Id = ((Number) operationAsList.get(1)).longValue();
            long person2Id = ((Number) operationAsList.get(2)).longValue();
            Date creationDate = new Date(((Number) operationAsList.get(3)).longValue());

            return new LdbcUpdate8AddFriendship(person1Id, person2Id, creationDate);
        }

        throw new SerializingMarshallingException(
                String.format("Workload does not know how to marshal operation\nWorkload: %s\nAssumed Operation Type: %s\nSerialized Operation: %s",
                        getClass().getName(),
                        operationAsList.get(0),
                        serializedOperation));
    }
}
