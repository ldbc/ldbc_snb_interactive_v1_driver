package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.ldbc.driver.*;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.ClassLoadingException;
import com.ldbc.driver.util.CsvFileReader;
import com.ldbc.driver.util.Function1;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import static com.ldbc.driver.generator.CsvEventStreamReader.EventReturnPolicy;

public class LdbcSnbInteractiveWorkload extends Workload {
    private List<CsvFileReader> forumUpdateOperationsFileReaders = new ArrayList<>();
    private List<CsvFileReader> personUpdateOperationsFileReaders = new ArrayList<>();

    private CsvFileReader readOperation1FileReader;
    private CsvFileReader readOperation2FileReader;
    private CsvFileReader readOperation3FileReader;
    private CsvFileReader readOperation4FileReader;
    private CsvFileReader readOperation5FileReader;
    private CsvFileReader readOperation6FileReader;
    private CsvFileReader readOperation7FileReader;
    private CsvFileReader readOperation8FileReader;
    private CsvFileReader readOperation9FileReader;
    private CsvFileReader readOperation10FileReader;
    private CsvFileReader readOperation11FileReader;
    private CsvFileReader readOperation12FileReader;
    private CsvFileReader readOperation13FileReader;
    private CsvFileReader readOperation14FileReader;

    private Duration readOperation1Interleave;
    private Duration readOperation2Interleave;
    private Duration readOperation3Interleave;
    private Duration readOperation4Interleave;
    private Duration readOperation5Interleave;
    private Duration readOperation6Interleave;
    private Duration readOperation7Interleave;
    private Duration readOperation8Interleave;
    private Duration readOperation9Interleave;
    private Duration readOperation10Interleave;
    private Duration readOperation11Interleave;
    private Duration readOperation12Interleave;
    private Duration readOperation13Interleave;
    private Duration readOperation14Interleave;

    private Map<Class, Duration> readOperationInterleaves;
    private Set<Class> enabledReadOperationTypes;
    private Set<Class<? extends Operation<?>>> enabledWriteOperationTypes;

    @Override
    public void onInit(Map<String, String> params) throws WorkloadException {
        List<String> compulsoryKeys = Lists.newArrayList(
                LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY);

        compulsoryKeys.addAll(LdbcSnbInteractiveConfiguration.READ_OPERATION_ENABLE_KEYS);
        compulsoryKeys.addAll(LdbcSnbInteractiveConfiguration.WRITE_OPERATION_ENABLE_KEYS);

        Set<String> missingPropertyParameters = LdbcSnbInteractiveConfiguration.missingPropertiesParameters(params, compulsoryKeys);
        if (false == missingPropertyParameters.isEmpty())
            throw new WorkloadException(String.format("Workload could not initialize due to missing parameters: %s", missingPropertyParameters.toString()));

        List<String> frequencyKeys = Lists.newArrayList(LdbcSnbInteractiveConfiguration.READ_OPERATION_FREQUENCY_KEYS);
        Set<String> missingFrequencyKeys = LdbcSnbInteractiveConfiguration.missingPropertiesParameters(params, frequencyKeys);
        if (false == missingFrequencyKeys.isEmpty()) {
            // if there are no frequencies set, there should be specified interleave times for read queries
            List<String> interleaveKeys = Lists.newArrayList(LdbcSnbInteractiveConfiguration.READ_OPERATION_INTERLEAVE_KEYS);
            Set<String> missingInterleaveKeys = LdbcSnbInteractiveConfiguration.missingPropertiesParameters(params, interleaveKeys);
            if (false == missingInterleaveKeys.isEmpty()) {
                throw new WorkloadException(String.format("Workload could not initialize. One of the following groups of parameters should be set: %s or %s", missingFrequencyKeys.toString(), missingInterleaveKeys.toString()));
            }
        } else {
            // if UPDATE_INTERLEAVE is missing, set it to DEFAULT
            Set<String> missingUpdateInterleave = LdbcSnbInteractiveConfiguration.missingPropertiesParameters(params, Lists.newArrayList(LdbcSnbInteractiveConfiguration.UPDATE_INTERLEAVE));
            if (false == missingUpdateInterleave.isEmpty()) {
                params.put(LdbcSnbInteractiveConfiguration.UPDATE_INTERLEAVE, LdbcSnbInteractiveConfiguration.DEFAULT_UPDATE_INTERLEAVE);
            }

            // compute interleave based on frequencies
            params = LdbcSnbInteractiveConfiguration.convertFrequenciesToInterleaves(params);
        }

        Iterable<String> forumUpdateFilePaths = (params.containsKey(LdbcSnbInteractiveConfiguration.FORUM_UPDATE_FILES))
                ?
                LdbcSnbInteractiveConfiguration.parseFilePathsListFromConfiguration(params.get(LdbcSnbInteractiveConfiguration.FORUM_UPDATE_FILES))
                :
                new ArrayList<String>();
        for (String forumUpdateFilePath : forumUpdateFilePaths) {
            File forumUpdateFile = new File(forumUpdateFilePath);
            try {
                CsvFileReader forumUpdateOperationsFileReader = new CsvFileReader(forumUpdateFile, LdbcSnbInteractiveConfiguration.PIPE_SEPARATOR);
                forumUpdateOperationsFileReaders.add(forumUpdateOperationsFileReader);
            } catch (FileNotFoundException e) {
                throw new WorkloadException("Unable to load forum update operation parameters file", e);
            }
        }

        Iterable<String> personUpdateFilePaths = (params.containsKey(LdbcSnbInteractiveConfiguration.PERSON_UPDATE_FILES))
                ?
                LdbcSnbInteractiveConfiguration.parseFilePathsListFromConfiguration(params.get(LdbcSnbInteractiveConfiguration.PERSON_UPDATE_FILES))
                :
                new ArrayList<String>();
        for (String personUpdateFilePath : personUpdateFilePaths) {
            File personUpdateFile = new File(personUpdateFilePath);
            try {
                CsvFileReader personUpdateOperationsFileReader = new CsvFileReader(personUpdateFile, LdbcSnbInteractiveConfiguration.PIPE_SEPARATOR);
                personUpdateOperationsFileReaders.add(personUpdateOperationsFileReader);
            } catch (FileNotFoundException e) {
                throw new WorkloadException("Unable to load person update operation parameters file", e);
            }
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
        try {
            readOperation1FileReader = new CsvFileReader(new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_1_PARAMS_FILENAME), LdbcSnbInteractiveConfiguration.PIPE_SEPARATOR);
            readOperation2FileReader = new CsvFileReader(new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_2_PARAMS_FILENAME), LdbcSnbInteractiveConfiguration.PIPE_SEPARATOR);
            readOperation3FileReader = new CsvFileReader(new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_3_PARAMS_FILENAME), LdbcSnbInteractiveConfiguration.PIPE_SEPARATOR);
            readOperation4FileReader = new CsvFileReader(new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_4_PARAMS_FILENAME), LdbcSnbInteractiveConfiguration.PIPE_SEPARATOR);
            readOperation5FileReader = new CsvFileReader(new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_5_PARAMS_FILENAME), LdbcSnbInteractiveConfiguration.PIPE_SEPARATOR);
            readOperation6FileReader = new CsvFileReader(new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_6_PARAMS_FILENAME), LdbcSnbInteractiveConfiguration.PIPE_SEPARATOR);
            readOperation7FileReader = new CsvFileReader(new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_7_PARAMS_FILENAME), LdbcSnbInteractiveConfiguration.PIPE_SEPARATOR);
            readOperation8FileReader = new CsvFileReader(new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_8_PARAMS_FILENAME), LdbcSnbInteractiveConfiguration.PIPE_SEPARATOR);
            readOperation9FileReader = new CsvFileReader(new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_9_PARAMS_FILENAME), LdbcSnbInteractiveConfiguration.PIPE_SEPARATOR);
            readOperation10FileReader = new CsvFileReader(new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_10_PARAMS_FILENAME), LdbcSnbInteractiveConfiguration.PIPE_SEPARATOR);
            readOperation11FileReader = new CsvFileReader(new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_11_PARAMS_FILENAME), LdbcSnbInteractiveConfiguration.PIPE_SEPARATOR);
            readOperation12FileReader = new CsvFileReader(new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_12_PARAMS_FILENAME), LdbcSnbInteractiveConfiguration.PIPE_SEPARATOR);
            readOperation13FileReader = new CsvFileReader(new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_13_PARAMS_FILENAME), LdbcSnbInteractiveConfiguration.PIPE_SEPARATOR);
            readOperation14FileReader = new CsvFileReader(new File(parametersDir, LdbcSnbInteractiveConfiguration.READ_OPERATION_14_PARAMS_FILENAME), LdbcSnbInteractiveConfiguration.PIPE_SEPARATOR);
        } catch (FileNotFoundException e) {
            throw new WorkloadException("Unable to load one of the read operation parameters files", e);
        }

        try {
            readOperation1Interleave = Duration.fromMilli(Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_1_INTERLEAVE_KEY)));
            readOperation2Interleave = Duration.fromMilli(Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_2_INTERLEAVE_KEY)));
            readOperation3Interleave = Duration.fromMilli(Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_3_INTERLEAVE_KEY)));
            readOperation4Interleave = Duration.fromMilli(Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_4_INTERLEAVE_KEY)));
            readOperation5Interleave = Duration.fromMilli(Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_5_INTERLEAVE_KEY)));
            readOperation6Interleave = Duration.fromMilli(Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_6_INTERLEAVE_KEY)));
            readOperation7Interleave = Duration.fromMilli(Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_7_INTERLEAVE_KEY)));
            readOperation8Interleave = Duration.fromMilli(Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_8_INTERLEAVE_KEY)));
            readOperation9Interleave = Duration.fromMilli(Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_9_INTERLEAVE_KEY)));
            readOperation10Interleave = Duration.fromMilli(Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_10_INTERLEAVE_KEY)));
            readOperation11Interleave = Duration.fromMilli(Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_11_INTERLEAVE_KEY)));
            readOperation12Interleave = Duration.fromMilli(Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_12_INTERLEAVE_KEY)));
            readOperation13Interleave = Duration.fromMilli(Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_13_INTERLEAVE_KEY)));
            readOperation14Interleave = Duration.fromMilli(Long.parseLong(params.get(LdbcSnbInteractiveConfiguration.READ_OPERATION_14_INTERLEAVE_KEY)));
        } catch (NumberFormatException e) {
            throw new WorkloadException("Unable to parse one of the read operation interleave values", e);
        }

        readOperationInterleaves = new HashMap<>();
        for (String readOperationInterleaveKey : LdbcSnbInteractiveConfiguration.READ_OPERATION_INTERLEAVE_KEYS) {
            String readOperationInterleaveString = params.get(readOperationInterleaveKey);
            Duration readOperationInterleaveDuration = Duration.fromMilli(Long.parseLong(readOperationInterleaveString));
            String readOperationClassName =
                    LdbcSnbInteractiveConfiguration.LDBC_INTERACTIVE_PACKAGE_PREFIX + LdbcSnbInteractiveConfiguration.removePrefix(
                            LdbcSnbInteractiveConfiguration.removeSuffix(
                                    readOperationInterleaveKey,
                                    LdbcSnbInteractiveConfiguration.INTERLEAVE_SUFFIX
                            ),
                            LdbcSnbInteractiveConfiguration.LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX
                    );
            try {
                Class readOperationClass = ClassLoaderHelper.loadClass(readOperationClassName);
                readOperationInterleaves.put(readOperationClass, readOperationInterleaveDuration);
            } catch (ClassLoadingException e) {
                throw new WorkloadException(
                        String.format("Unable to load operation class for parameter: %s\nGuessed incorrect class name: %s",
                                readOperationInterleaveKey, readOperationClassName),
                        e
                );
            }
        }

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
    }

    @Override
    protected void onCleanup() throws WorkloadException {
        for (CsvFileReader forumUpdateOperationsFileReader : forumUpdateOperationsFileReaders) {
            forumUpdateOperationsFileReader.closeReader();
        }

        for (CsvFileReader personUpdateOperationsFileReader : personUpdateOperationsFileReaders) {
            personUpdateOperationsFileReader.closeReader();
        }

        readOperation1FileReader.closeReader();
        readOperation2FileReader.closeReader();
        readOperation3FileReader.closeReader();
        readOperation4FileReader.closeReader();
        readOperation5FileReader.closeReader();
        readOperation6FileReader.closeReader();
        readOperation7FileReader.closeReader();
        readOperation8FileReader.closeReader();
        readOperation9FileReader.closeReader();
        readOperation10FileReader.closeReader();
        readOperation11FileReader.closeReader();
        readOperation12FileReader.closeReader();
        readOperation13FileReader.closeReader();
        readOperation14FileReader.closeReader();
    }

    @Override
    protected WorkloadStreams getStreams(GeneratorFactory gf) throws WorkloadException {
        Time workloadStartTime = null;
        WorkloadStreams ldbcSnbInteractiveWorkloadStreams = new WorkloadStreams();

        /* *******
         * *******
         * *******
         *  WRITES
         * *******
         * *******
         * *******/

        Set<Class<? extends Operation<?>>> dependentAsynchronousOperationTypes = Sets.<Class<? extends Operation<?>>>newHashSet(
                LdbcUpdate1AddPerson.class,
                LdbcUpdate8AddFriendship.class
        );
        List<Iterator<?>> asynchronousDependencyStreamsList = new ArrayList<>();
        List<Iterator<?>> asynchronousNonDependencyStreamsList = new ArrayList<>();

        /*
         * Create forum write operation streams
         */
        for (CsvFileReader forumUpdateOperationsFileReader : forumUpdateOperationsFileReaders) {
            PeekingIterator<Operation<?>> unfilteredForumUpdateOperations = Iterators.peekingIterator(
                    new WriteEventStreamReader(forumUpdateOperationsFileReader, EventReturnPolicy.AT_LEAST_ONE_MATCH)
            );
            try {
                if (null == workloadStartTime || unfilteredForumUpdateOperations.peek().scheduledStartTime().lt(workloadStartTime)) {
                    workloadStartTime = unfilteredForumUpdateOperations.peek().scheduledStartTime();
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
            // TODO
            // TODO
            // TODO
            // TODO this needs to be changed, if we get SafeT as input again we could set dependencyTime = scheduledStartTime - SafeT
            // TODO
            // TODO
            // TODO
            Iterator<Operation<?>> filteredForumUpdateOperationsWithDependencyTimes = gf.assignDependencyTimes(
                    gf.constant(Time.fromMilli(0)),
                    filteredForumUpdateOperations
            );

            Set<Class<? extends Operation<?>>> dependentForumUpdateOperationTypes = Sets.<Class<? extends Operation<?>>>newHashSet(
                    LdbcUpdate2AddPostLike.class,
                    LdbcUpdate3AddCommentLike.class,
                    LdbcUpdate4AddForum.class,
                    LdbcUpdate5AddForumMembership.class,
                    LdbcUpdate6AddPost.class,
                    LdbcUpdate7AddComment.class
            );

            ldbcSnbInteractiveWorkloadStreams.addBlockingStream(
                    dependentForumUpdateOperationTypes,
                    Collections.<Operation<?>>emptyIterator(),
                    filteredForumUpdateOperationsWithDependencyTimes
            );
        }

        for (CsvFileReader personUpdateOperationsFileReader : personUpdateOperationsFileReaders) {
            PeekingIterator<Operation<?>> unfilteredPersonUpdateOperations = Iterators.peekingIterator(
                    new WriteEventStreamReader(personUpdateOperationsFileReader, EventReturnPolicy.AT_LEAST_ONE_MATCH)
            );
            try {
                if (null == workloadStartTime || unfilteredPersonUpdateOperations.peek().scheduledStartTime().lt(workloadStartTime)) {
                    workloadStartTime = unfilteredPersonUpdateOperations.peek().scheduledStartTime();
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
            // TODO
            // TODO
            // TODO
            // TODO this needs to be changed, if we get SafeT as input again we could set dependencyTime = scheduledStartTime - SafeT
            // TODO
            // TODO
            // TODO
            Iterator<Operation<?>> filteredPersonUpdateOperationsWithDependencyTimes = gf.assignDependencyTimes(
                    gf.constant(Time.fromMilli(0)),
                    filteredPersonUpdateOperations
            );

            asynchronousDependencyStreamsList.add(filteredPersonUpdateOperationsWithDependencyTimes);
        }

        if (null == workloadStartTime) workloadStartTime = Time.fromMilli(0);

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
        Iterator<Operation<?>> operation1StreamWithoutTimes = new Query1EventStreamReader(gf.repeating(readOperation1FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        Iterator<Time> operation1StartTimes = gf.constantIncrementTime(workloadStartTime.plus(readOperation1Interleave), readOperation1Interleave);
        Iterator<Operation<?>> readOperation1Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation1StartTimes, operation1StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation2StreamWithoutTimes = new Query2EventStreamReader(gf.repeating(readOperation2FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        Iterator<Time> operation2StartTimes = gf.constantIncrementTime(workloadStartTime.plus(readOperation2Interleave), readOperation2Interleave);
        Iterator<Operation<?>> readOperation2Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation2StartTimes, operation2StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation3StreamWithoutTimes = new Query3EventStreamReader(gf.repeating(readOperation3FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        Iterator<Time> operation3StartTimes = gf.constantIncrementTime(workloadStartTime.plus(readOperation3Interleave), readOperation3Interleave);
        Iterator<Operation<?>> readOperation3Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation3StartTimes, operation3StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation4StreamWithoutTimes = new Query4EventStreamReader(gf.repeating(readOperation4FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        Iterator<Time> operation4StartTimes = gf.constantIncrementTime(workloadStartTime.plus(readOperation4Interleave), readOperation4Interleave);
        Iterator<Operation<?>> readOperation4Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation4StartTimes, operation4StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation5StreamWithoutTimes = new Query5EventStreamReader(gf.repeating(readOperation5FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        Iterator<Time> operation5StartTimes = gf.constantIncrementTime(workloadStartTime.plus(readOperation5Interleave), readOperation5Interleave);
        Iterator<Operation<?>> readOperation5Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation5StartTimes, operation5StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation6StreamWithoutTimes = new Query6EventStreamReader(gf.repeating(readOperation6FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        Iterator<Time> operation6StartTimes = gf.constantIncrementTime(workloadStartTime.plus(readOperation6Interleave), readOperation6Interleave);
        Iterator<Operation<?>> readOperation6Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation6StartTimes, operation6StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation7StreamWithoutTimes = new Query7EventStreamReader(gf.repeating(readOperation7FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        Iterator<Time> operation7StartTimes = gf.constantIncrementTime(workloadStartTime.plus(readOperation7Interleave), readOperation7Interleave);
        Iterator<Operation<?>> readOperation7Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation7StartTimes, operation7StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation8StreamWithoutTimes = new Query8EventStreamReader(gf.repeating(readOperation8FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        Iterator<Time> operation8StartTimes = gf.constantIncrementTime(workloadStartTime.plus(readOperation8Interleave), readOperation8Interleave);
        Iterator<Operation<?>> readOperation8Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation8StartTimes, operation8StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation9StreamWithoutTimes = new Query9EventStreamReader(gf.repeating(readOperation9FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        Iterator<Time> operation9StartTimes = gf.constantIncrementTime(workloadStartTime.plus(readOperation9Interleave), readOperation9Interleave);
        Iterator<Operation<?>> readOperation9Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation9StartTimes, operation9StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation10StreamWithoutTimes = new Query10EventStreamReader(gf.repeating(readOperation10FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        Iterator<Time> operation10StartTimes = gf.constantIncrementTime(workloadStartTime.plus(readOperation10Interleave), readOperation10Interleave);
        Iterator<Operation<?>> readOperation10Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation10StartTimes, operation10StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation11StreamWithoutTimes = new Query11EventStreamReader(gf.repeating(readOperation11FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        Iterator<Time> operation11StartTimes = gf.constantIncrementTime(workloadStartTime.plus(readOperation11Interleave), readOperation11Interleave);
        Iterator<Operation<?>> readOperation11Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation11StartTimes, operation11StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation12StreamWithoutTimes = new Query12EventStreamReader(gf.repeating(readOperation12FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        Iterator<Time> operation12StartTimes = gf.constantIncrementTime(workloadStartTime.plus(readOperation12Interleave), readOperation12Interleave);
        Iterator<Operation<?>> readOperation12Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation12StartTimes, operation12StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation13StreamWithoutTimes = new Query13EventStreamReader(gf.repeating(readOperation13FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        Iterator<Time> operation13StartTimes = gf.constantIncrementTime(workloadStartTime.plus(readOperation13Interleave), readOperation13Interleave);
        Iterator<Operation<?>> readOperation13Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation13StartTimes, operation13StreamWithoutTimes)
        );

        Iterator<Operation<?>> operation14StreamWithoutTimes = new Query14EventStreamReader(gf.repeating(readOperation14FileReader), EventReturnPolicy.AT_LEAST_ONE_MATCH);
        Iterator<Time> operation14StartTimes = gf.constantIncrementTime(workloadStartTime.plus(readOperation14Interleave), readOperation14Interleave);
        Iterator<Operation<?>> readOperation14Stream = gf.assignDependencyTimes(
                gf.constant(workloadStartTime),
                gf.assignStartTimes(operation14StartTimes, operation14StreamWithoutTimes)
        );

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
        Iterator<Operation<?>> asynchronousDependencyStreams = gf.mergeSortOperationsByStartTime(
                asynchronousDependencyStreamsList.toArray(new Iterator[asynchronousDependencyStreamsList.size()])
        );
        /*
         * Merge all non dependency asynchronous operation streams, ordered by operation start times
         */
        Iterator<Operation<?>> asynchronousNonDependencyStreams = gf.mergeSortOperationsByStartTime(
                asynchronousNonDependencyStreamsList.toArray(new Iterator[asynchronousNonDependencyStreamsList.size()])
        );

        /* **************
         * **************
         * **************
         *  FINAL STREAMS
         * **************
         * **************
         * **************/

        ldbcSnbInteractiveWorkloadStreams.setAsynchronousStream(
                dependentAsynchronousOperationTypes,
                asynchronousDependencyStreams,
                asynchronousNonDependencyStreams
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
         *
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
                if (isEmptyResult) return DbValidationParametersFilterResult.REJECT_AND_CONTINUE;

                long remainingRequiredResultsForOperationType = remainingRequiredResultsPerOperationType.get(operationType) - 1;

                if (0 == remainingRequiredResultsForOperationType)
                    remainingRequiredResultsPerOperationType.remove(operationType);
                else
                    remainingRequiredResultsPerOperationType.put(operationType, remainingRequiredResultsForOperationType);

                if (remainingRequiredResultsPerOperationType.size() > 0)
                    return DbValidationParametersFilterResult.ACCEPT_AND_CONTINUE;
                else
                    return DbValidationParametersFilterResult.ACCEPT_AND_FINISH;
            }
        };
    }

    @Override
    public Duration maxExpectedInterleave() {
        return Duration.fromHours(1);
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
            operationAsList.add(ldbcQuery.personUri());
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
            operationAsList.add(ldbcQuery.personUri());
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
            operationAsList.add(ldbcQuery.personUri());
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
            operationAsList.add(ldbcQuery.personUri());
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
            operationAsList.add(ldbcQuery.personUri());
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
            operationAsList.add(ldbcQuery.personUri());
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
            operationAsList.add(ldbcQuery.personUri());
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
            operationAsList.add(ldbcQuery.personUri());
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
            operationAsList.add(ldbcQuery.personUri());
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
            operationAsList.add(ldbcQuery.personUri());
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
            operationAsList.add(ldbcQuery.personUri());
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
            operationAsList.add(ldbcQuery.personUri());
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
            operationAsList.add(ldbcQuery.person1Uri());
            operationAsList.add(ldbcQuery.person2Id());
            operationAsList.add(ldbcQuery.person2Uri());
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
            operationAsList.add(ldbcQuery.person1Uri());
            operationAsList.add(ldbcQuery.person2Id());
            operationAsList.add(ldbcQuery.person2Uri());
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
            operationAsList.add(ldbcQuery.creationDate().getTime());
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
            String personUri = (String) operationAsList.get(2);
            String firstName = (String) operationAsList.get(3);
            int limit = ((Number) operationAsList.get(4)).intValue();
            return new LdbcQuery1(personId, personUri, firstName, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery2.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            Date maxDate = new Date(((Number) operationAsList.get(3)).longValue());
            int limit = ((Number) operationAsList.get(4)).intValue();
            return new LdbcQuery2(personId, personUri, maxDate, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery3.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            String countryXName = (String) operationAsList.get(3);
            String countryYName = (String) operationAsList.get(4);
            Date startDate = new Date(((Number) operationAsList.get(5)).longValue());
            int durationDays = ((Number) operationAsList.get(6)).intValue();
            int limit = ((Number) operationAsList.get(7)).intValue();
            return new LdbcQuery3(personId, personUri, countryXName, countryYName, startDate, durationDays, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery4.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            Date startDate = new Date(((Number) operationAsList.get(3)).longValue());
            int durationDays = ((Number) operationAsList.get(4)).intValue();
            int limit = ((Number) operationAsList.get(5)).intValue();
            return new LdbcQuery4(personId, personUri, startDate, durationDays, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery5.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            Date minDate = new Date(((Number) operationAsList.get(3)).longValue());
            int limit = ((Number) operationAsList.get(4)).intValue();
            return new LdbcQuery5(personId, personUri, minDate, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery6.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            String tagName = (String) operationAsList.get(3);
            int limit = ((Number) operationAsList.get(4)).intValue();
            return new LdbcQuery6(personId, personUri, tagName, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery7.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            int limit = ((Number) operationAsList.get(3)).intValue();
            return new LdbcQuery7(personId, personUri, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery8.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            int limit = ((Number) operationAsList.get(3)).intValue();
            return new LdbcQuery8(personId, personUri, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery9.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            Date maxDate = new Date(((Number) operationAsList.get(3)).longValue());
            int limit = ((Number) operationAsList.get(4)).intValue();
            return new LdbcQuery9(personId, personUri, maxDate, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery10.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            int month = ((Number) operationAsList.get(3)).intValue();
            int limit = ((Number) operationAsList.get(4)).intValue();
            return new LdbcQuery10(personId, personUri, month, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery11.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            String countryName = (String) operationAsList.get(3);
            int workFromYear = ((Number) operationAsList.get(4)).intValue();
            int limit = ((Number) operationAsList.get(5)).intValue();
            return new LdbcQuery11(personId, personUri, countryName, workFromYear, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery12.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String personUri = (String) operationAsList.get(2);
            String tagClassName = (String) operationAsList.get(3);
            int limit = ((Number) operationAsList.get(4)).intValue();
            return new LdbcQuery12(personId, personUri, tagClassName, limit);
        }

        if (operationAsList.get(0).equals(LdbcQuery13.class.getName())) {
            long person1Id = ((Number) operationAsList.get(1)).longValue();
            String person1Uri = (String) operationAsList.get(2);
            long person2Id = ((Number) operationAsList.get(3)).longValue();
            String person2Uri = (String) operationAsList.get(4);
            return new LdbcQuery13(person1Id, person1Uri, person2Id, person2Uri);
        }

        if (operationAsList.get(0).equals(LdbcQuery14.class.getName())) {
            long person1Id = ((Number) operationAsList.get(1)).longValue();
            String person1Uri = (String) operationAsList.get(2);
            long person2Id = ((Number) operationAsList.get(3)).longValue();
            String person2Uri = (String) operationAsList.get(4);
            return new LdbcQuery14(person1Id, person1Uri, person2Id, person2Uri);
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
