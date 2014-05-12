package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationClassification;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.StartTimeAssigningOperationGenerator;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.ClassLoadingException;
import com.ldbc.driver.util.GeneratorUtils;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple.Tuple2;

import java.io.File;
import java.util.*;

import static com.ldbc.driver.OperationClassification.GctMode;
import static com.ldbc.driver.OperationClassification.SchedulingMode;

public class LdbcInteractiveWorkload extends Workload {
    public final static String WRITE_STREAM_FILENAME_KEY = "updates";
    public final static String PARAMETERS_FILENAME_KEY = "parameters";
    public final static String INTERLEAVE_DURATION_KEY = "interleave_duration";
    public final static String READ_RATIO_KEY = "read_ratio";
    public final static String WRITE_RATIO_KEY = "write_ratio";

    public final static String READ_OPERATION_1_KEY = LdbcQuery1.class.getName();
    public final static String READ_OPERATION_2_KEY = LdbcQuery2.class.getName();
    public final static String READ_OPERATION_3_KEY = LdbcQuery3.class.getName();
    public final static String READ_OPERATION_4_KEY = LdbcQuery4.class.getName();
    public final static String READ_OPERATION_5_KEY = LdbcQuery5.class.getName();
    public final static String READ_OPERATION_6_KEY = LdbcQuery6.class.getName();
    public final static String READ_OPERATION_7_KEY = LdbcQuery7.class.getName();
    public final static String READ_OPERATION_8_KEY = LdbcQuery8.class.getName();
    public final static String READ_OPERATION_9_KEY = LdbcQuery9.class.getName();
    public final static String READ_OPERATION_10_KEY = LdbcQuery10.class.getName();
    public final static String READ_OPERATION_11_KEY = LdbcQuery11.class.getName();
    public final static String READ_OPERATION_12_KEY = LdbcQuery12.class.getName();
    public final static String READ_OPERATION_13_KEY = LdbcQuery13.class.getName();
    public final static String READ_OPERATION_14_KEY = LdbcQuery14.class.getName();
    public final static List<String> READ_OPERATION_KEYS = Lists.newArrayList(
            READ_OPERATION_1_KEY,
            READ_OPERATION_2_KEY,
            READ_OPERATION_3_KEY,
            READ_OPERATION_4_KEY,
            READ_OPERATION_5_KEY,
            READ_OPERATION_6_KEY,
            READ_OPERATION_7_KEY,
            READ_OPERATION_8_KEY,
            READ_OPERATION_9_KEY,
            READ_OPERATION_10_KEY,
            READ_OPERATION_11_KEY,
            READ_OPERATION_12_KEY,
            READ_OPERATION_13_KEY,
            READ_OPERATION_14_KEY);

    public final static String WRITE_OPERATION_1_KEY = LdbcUpdate1AddPerson.class.getName();
    public final static String WRITE_OPERATION_2_KEY = LdbcUpdate2AddPostLike.class.getName();
    public final static String WRITE_OPERATION_3_KEY = LdbcUpdate3AddCommentLike.class.getName();
    public final static String WRITE_OPERATION_4_KEY = LdbcUpdate4AddForum.class.getName();
    public final static String WRITE_OPERATION_5_KEY = LdbcUpdate5AddForumMembership.class.getName();
    public final static String WRITE_OPERATION_6_KEY = LdbcUpdate6AddPost.class.getName();
    public final static String WRITE_OPERATION_7_KEY = LdbcUpdate7AddComment.class.getName();
    public final static String WRITE_OPERATION_8_KEY = LdbcUpdate8AddFriendship.class.getName();
    public final static List<String> WRITE_OPERATION_KEYS = Lists.newArrayList(
            WRITE_OPERATION_1_KEY,
            WRITE_OPERATION_2_KEY,
            WRITE_OPERATION_3_KEY,
            WRITE_OPERATION_4_KEY,
            WRITE_OPERATION_5_KEY,
            WRITE_OPERATION_6_KEY,
            WRITE_OPERATION_7_KEY,
            WRITE_OPERATION_8_KEY
    );

    private WriteEventStreamReader writeOperations;
    private SubstitutionParameters substitutionParameters;
    private Duration interleaveDuration;
    private double readRatio;
    private double writeRatio;
    private Map<Class, Double> readOperationRatios;
    private Set<Class> writeOperationFilter;

    @Override
    public Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications() {
        Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications = new HashMap<Class<? extends Operation<?>>, OperationClassification>();
        // TODO use correct operation classifications
        operationClassifications.put(LdbcQuery1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery3.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery4.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery5.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery6.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery7.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery8.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery9.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery10.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery11.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery12.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery13.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcQuery14.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcUpdate1AddPerson.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcUpdate2AddPostLike.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcUpdate3AddCommentLike.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcUpdate4AddForum.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcUpdate5AddForumMembership.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcUpdate6AddPost.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcUpdate7AddComment.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        operationClassifications.put(LdbcUpdate8AddFriendship.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.NONE));
        return operationClassifications;
    }

    @Override
    public void onInit(Map<String, String> properties) throws WorkloadException {
        List<String> compulsoryKeys = Lists.newArrayList(
                WRITE_STREAM_FILENAME_KEY,
                PARAMETERS_FILENAME_KEY,
                INTERLEAVE_DURATION_KEY,
                READ_RATIO_KEY,
                WRITE_RATIO_KEY);
        compulsoryKeys.addAll(READ_OPERATION_KEYS);
        compulsoryKeys.addAll(WRITE_OPERATION_KEYS);

        List<String> missingPropertyParameters = missingPropertiesParameters(properties, compulsoryKeys);
        if (false == missingPropertyParameters.isEmpty())
            throw new WorkloadException(String.format("Workload could not initialize due to missing parameters: %s", missingPropertyParameters.toString()));

        String writesFilename = properties.get(WRITE_STREAM_FILENAME_KEY);
        if (false == new File(writesFilename).exists()) {
            throw new WorkloadException(String.format("Write events file does not exist: %s", writesFilename));
        }
        File writesFile = new File(writesFilename);
        try {
            writeOperations = new WriteEventStreamReader(writesFile);
        } catch (Exception e) {
            throw new WorkloadException(String.format("Unable to load write event stream from: %s", writesFile.getAbsolutePath()), e);
        }

        String parametersFilename = properties.get(PARAMETERS_FILENAME_KEY);
        if (false == new File(parametersFilename).exists()) {
            throw new WorkloadException(String.format("Substitution parameters file does not exist: %s", parametersFilename));
        }
        File parametersFile = new File(parametersFilename);
        try {
            substitutionParameters = SubstitutionParameters.fromJson(parametersFile);
        } catch (Exception e) {
            throw new WorkloadException(String.format("Unable to load substitution parameters from: %s", parametersFile.getAbsolutePath()), e);
        }

        try {
            long interleaveDurationMs = Long.parseLong(properties.get(INTERLEAVE_DURATION_KEY));
            interleaveDuration = Duration.fromMilli(interleaveDurationMs);
        } catch (NumberFormatException e) {
            throw new WorkloadException(String.format("Unable to parse interleave duration: %s", properties.get(INTERLEAVE_DURATION_KEY)), e);
        }

        try {
            readRatio = Double.parseDouble(properties.get(READ_RATIO_KEY));
        } catch (NumberFormatException e) {
            throw new WorkloadException(String.format("Unable to parse read ratio: %s", properties.get(READ_RATIO_KEY)), e);
        }

        try {
            writeRatio = Double.parseDouble(properties.get(WRITE_RATIO_KEY));
        } catch (NumberFormatException e) {
            throw new WorkloadException(String.format("Unable to parse write ratio: %s", properties.get(WRITE_RATIO_KEY)), e);
        }

        readOperationRatios = new HashMap<>();
        for (String readOperationKey : READ_OPERATION_KEYS) {
            String operationRatioString = properties.get(readOperationKey);
            Double operationRatio = Double.parseDouble(operationRatioString);
            try {
                Class operationClass = ClassLoaderHelper.loadClass(readOperationKey);
                readOperationRatios.put(operationClass, operationRatio);
            } catch (ClassLoadingException e) {
                throw new WorkloadException(String.format("Unable to load operation class: %s", readOperationKey), e);
            }
        }

        writeOperationFilter = new HashSet<>();
        for (String writeOperationKey : WRITE_OPERATION_KEYS) {
            String writeOperationEnabledString = properties.get(writeOperationKey);
            Boolean writeOperationEnabled = Boolean.parseBoolean(writeOperationEnabledString);
            try {
                Class operationClass = ClassLoaderHelper.loadClass(writeOperationKey);
                if (writeOperationEnabled) writeOperationFilter.add(operationClass);
            } catch (ClassLoadingException e) {
                throw new WorkloadException(String.format("Unable to load operation class: %s", writeOperationKey), e);
            }
        }
    }

    private List<String> missingPropertiesParameters
            (Map<String, String> properties, List<String> compulsoryPropertyKeys) {
        List<String> missingPropertyKeys = new ArrayList<String>();
        for (String compulsoryKey : compulsoryPropertyKeys) {
            if (null == properties.get(compulsoryKey)) missingPropertyKeys.add(compulsoryKey);
        }
        return missingPropertyKeys;
    }

    @Override
    protected void onCleanup() throws WorkloadException {
    }

    @Override
    protected Iterator<Operation<?>> createOperations(GeneratorFactory generators) throws WorkloadException {
        Iterator<String> firstNameGenerator = generators.discrete(substitutionParameters.firstNames);
        Iterator<Long> personIdGenerator = generators.uniform(substitutionParameters.minPersonId, substitutionParameters.maxPersonId);
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(Calendar.YEAR, substitutionParameters.minPostCreationDate);
        long postCreationDateAsMs000 = c.getTimeInMillis();
        c.clear();
        c.set(Calendar.YEAR, substitutionParameters.maxPostCreationDate);
        long postCreationDateAsMs100 = c.getTimeInMillis();
        long postCreationDateRangeAsMs100 = postCreationDateAsMs100 - postCreationDateAsMs000;
        long postCreationDateAsMs033 = postCreationDateAsMs000 + Math.round(postCreationDateRangeAsMs100 * 0.33);
        long postCreationDateAsMs066 = postCreationDateAsMs000 + Math.round(postCreationDateRangeAsMs100 * 0.66);
        long postCreationDateAsMs095 = postCreationDateAsMs000 + Math.round(postCreationDateRangeAsMs100 * 0.95);
        long postCreationDateRangeAsMs002 = Math.round(postCreationDateRangeAsMs100 * 0.02);
        long postCreationDateRangeAsMs004 = Math.round(postCreationDateRangeAsMs100 * 0.04);
        long postCreationDateRangeAsMs033 = Math.round(postCreationDateRangeAsMs100 * 0.33);
        Iterator<Long> postCreationDateGenerator00_66 = generators.uniform(postCreationDateAsMs000, postCreationDateAsMs066);
        Iterator<Long> postCreationDateGenerator33_66 = generators.uniform(postCreationDateAsMs033, postCreationDateAsMs066);
        Iterator<Long> postCreationDateGenerator00_95 = generators.uniform(postCreationDateAsMs000, postCreationDateAsMs095);
        Iterator<String[]> countryPairsGenerator = generators.discrete(substitutionParameters.countryPairs);
        Iterator<Long> postCreationDateRangeDuration02_04 = generators.uniform(postCreationDateRangeAsMs002, postCreationDateRangeAsMs004);
        Iterator<String> tagNameGenerator = generators.discrete(substitutionParameters.tagNames);
        Iterator<Integer> horoscopeGenerator = generators.uniform(1, 12);
        Iterator<String> countriesGenerator = generators.discrete(substitutionParameters.countries);
        Iterator<Integer> workFromYearGenerator00_100 = generators.uniform(substitutionParameters.minWorkFrom, substitutionParameters.maxWorkFrom);

        Iterator<String> tagClassesGenerator = generators.discrete(substitutionParameters.tagClasses);

        /*
         * Create Generators for desired Operations
         */
        List<Tuple2<Double, Iterator<Operation<?>>>> readOperationsMix = new ArrayList<>();

        /*
         * Operation 1
         *  - Select uniformly randomly from person first names
         */
        int operation1Limit = LdbcQuery1.DEFAULT_LIMIT;
        readOperationsMix.add(Tuple.tuple2(
                readOperationRatios.get(LdbcQuery1.class),
                (Iterator<Operation<?>>) new Query1Generator(personIdGenerator, firstNameGenerator, operation1Limit)));

        /*
         * Operation 2
         *  - Person ID - select uniformly randomly from person ids
         *  - Post Creation Date - select uniformly randomly a post creation date from between 33perc-66perc of entire date range
         */
        int operation2Limit = LdbcQuery2.DEFAULT_LIMIT;
        readOperationsMix.add(Tuple.tuple2(
                readOperationRatios.get(LdbcQuery2.class),
                (Iterator<Operation<?>>) new Query2Generator(personIdGenerator, postCreationDateGenerator33_66, operation2Limit)));

        /*
         * Operation 3
         *  - Person ID - select uniformly randomly from person ids
         *  - Post Creation Date - select uniformly randomly a post creation date from between 0perc-66perc of entire date range
         *  - Duration - a number of days (33% of the length of post creation date range)
         *  - Country1 - the first of country pair
         *  - Country2 - the second of country pair
         */
        readOperationsMix.add(Tuple.tuple2(
                readOperationRatios.get(LdbcQuery3.class),
                (Iterator<Operation<?>>) new Query3Generator(personIdGenerator, countryPairsGenerator, postCreationDateGenerator00_66, postCreationDateRangeAsMs033)));

        /*
         * Operation 4
         * - Person ID - select uniformly randomly from person ids
         * - Post Creation Date - select uniformly randomly a post creation date from between 0perc-95perc of entire date range
         * - Duration - a uniformly randomly selected duration between 2% and 4% of the length of post creation date range        
         */
        readOperationsMix.add(Tuple.tuple2(
                readOperationRatios.get(LdbcQuery4.class),
                (Iterator<Operation<?>>) new Query4Generator(personIdGenerator, postCreationDateGenerator00_95, postCreationDateRangeDuration02_04)));

        // TODO http://www.ldbc.eu:8090/display/TUC/IW+Substitution+parameters+selection claims Q5 needs duration parameter
        // TODO http://www.ldbc.eu:8090/display/TUC/Interactive+Workload does not show that parameters
        // TODO add duration to operation if it found that it's necessary
        /*
         * Operation 5
         * - Person - select uniformly randomly from person ids
         * - Join Date - select uniformly randomly a post creation date from between 0perc-95perc of entire date range
         */
        readOperationsMix.add(Tuple.tuple2(
                readOperationRatios.get(LdbcQuery5.class),
                (Iterator<Operation<?>>) new Query5Generator(personIdGenerator, postCreationDateGenerator00_95)));

        /*
         * Operation 6
         * - Person - select uniformly randomly from person ids
         * - Tag - select uniformly randomly from tag uris
         */
        int operation6Limit = LdbcQuery6.DEFAULT_LIMIT;
        readOperationsMix.add(Tuple.tuple2(
                readOperationRatios.get(LdbcQuery6.class),
                (Iterator<Operation<?>>) new Query6Generator(personIdGenerator, tagNameGenerator, operation6Limit)));

        /*
         * Operation 7
         * Person - select uniformly randomly from person ids
         */
        int operation7Limit = LdbcQuery7.DEFAULT_LIMIT;
        readOperationsMix.add(Tuple.tuple2(
                readOperationRatios.get(LdbcQuery7.class),
                (Iterator<Operation<?>>) new Query7Generator(personIdGenerator, operation7Limit)));

        /*
         * Operation 8
         * Person - select uniformly randomly from person ids
         */
        int operation8Limit = LdbcQuery8.DEFAULT_LIMIT;
        readOperationsMix.add(Tuple.tuple2(
                readOperationRatios.get(LdbcQuery8.class),
                (Iterator<Operation<?>>) new Query8Generator(personIdGenerator, operation8Limit)));

        /*
         * Operation 9
         * Person - select uniformly randomly from person ids
         * Date - select uniformly randomly a post creation date from between 33perc-66perc of entire date range
         */
        int operation9Limit = LdbcQuery9.DEFAULT_LIMIT;
        readOperationsMix.add(Tuple.tuple2(
                readOperationRatios.get(LdbcQuery9.class),
                (Iterator<Operation<?>>) new Query9Generator(personIdGenerator, postCreationDateGenerator33_66, operation9Limit)));

        /*
         * Operation 10
         * Person - select uniformly randomly from person ids
         * HS0 - select uniformly randomly a horoscope sign (a random number between 1 and 12)
         * HS1 - HS0 + 1 (but 12 + 1 = 1)
         */
        int operation10Limit = LdbcQuery10.DEFAULT_LIMIT;
        readOperationsMix.add(Tuple.tuple2(
                readOperationRatios.get(LdbcQuery10.class),
                (Iterator<Operation<?>>) new Query10Generator(personIdGenerator, horoscopeGenerator, operation10Limit)));

        /*
         * Operation 11
         * Person - select uniformly randomly from person ids
         * // TODO parameter file does not have country dont have IDS only names
         * Country - select uniformly randomly from country ids
         * Date - a random date from 0% to 100% of whole workFrom timeline
         */
        int operation11Limit = LdbcQuery11.DEFAULT_LIMIT;
        readOperationsMix.add(Tuple.tuple2(
                readOperationRatios.get(LdbcQuery11.class),
                (Iterator<Operation<?>>) new Query11Generator(personIdGenerator, countriesGenerator, workFromYearGenerator00_100, operation11Limit)));

        /*
         * Operation 12
         * Person - select uniformly randomly from person ids
         * TagType - select uniformly randomly tagTypeURI (used files: tagTypes.txt and tagTypes.sql)
         */
        int operation12Limit = LdbcQuery12.DEFAULT_LIMIT;
        readOperationsMix.add(Tuple.tuple2(
                readOperationRatios.get(LdbcQuery12.class),
                (Iterator<Operation<?>>) new Query12Generator(personIdGenerator, tagClassesGenerator, operation12Limit)));

        /*
         * Operation 13
         * Person1 - start person
         * Person1 - end person
         */
        readOperationsMix.add(Tuple.tuple2(
                readOperationRatios.get(LdbcQuery13.class),
                (Iterator<Operation<?>>) new Query13Generator(personIdGenerator)));

        /*
         * Operation 14
         * Person1 - start person
         * Person1 - end person
         */
        readOperationsMix.add(Tuple.tuple2(
                readOperationRatios.get(LdbcQuery14.class),
                (Iterator<Operation<?>>) new Query14Generator(personIdGenerator)));

        /*
         * Create Discrete Generator from read operation mix
         */
        Iterator<Operation<?>> readOperations = generators.weightedDiscreteDereferencing(readOperationsMix);

        /*
         * Add scheduled start time to read queries
         */
        Iterator<Time> startTimeGenerator = GeneratorUtils.constantIncrementStartTimeGenerator(generators, Time.now(), interleaveDuration);
        Iterator<Operation<?>> readOperationsWithTime = new StartTimeAssigningOperationGenerator(startTimeGenerator, readOperations);

        /*
         * Filter Write Operations
         */
        Predicate<Operation<?>> allowedWriteOperationsFilter = new Predicate<Operation<?>>() {
            @Override
            public boolean apply(Operation<?> operation) {
                return writeOperationFilter.contains(operation.getClass());
            }
        };
        Iterator<Operation<?>> filteredWriteOperations = Iterators.filter(writeOperations, allowedWriteOperationsFilter);

        /*
         * Move scheduled start times of write operations to now
         */
        // TODO need separate parameter for: write operation time shift, write operation compression, read operation interleave/load
        Iterator<Operation<?>> filteredWriteOperationsTimeShiftedToNow = generators.timeOffset(filteredWriteOperations, Time.now());

        /*
         * Mix read and write operations
         */
        List<Tuple2<Double, Iterator<Operation<?>>>> readWriteOperationMix = Lists.newArrayList(
                Tuple.tuple2(readRatio, readOperationsWithTime),
                Tuple.tuple2(writeRatio, filteredWriteOperationsTimeShiftedToNow)
        );
        Iterator<Operation<?>> readAndWriteOperations = generators.weightedDiscreteDereferencing(readWriteOperationMix);

//        return readAndWriteOperations;
        // TODO remove, this is just to make sure start times are assigned to all operations
        return new StartTimeAssigningOperationGenerator(startTimeGenerator, readAndWriteOperations);
    }

    class Query1Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIds;
        private final Iterator<String> firstNames;
        private final int limit;

        protected Query1Generator(Iterator<Long> personIds, Iterator<String> firstNames, int limit) {
            this.personIds = personIds;
            this.firstNames = firstNames;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery1(personIds.next(), firstNames.next(), limit);
        }
    }

    class Query2Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIds;
        private final Iterator<Long> postCreationDates;
        private final int limit;

        protected Query2Generator(Iterator<Long> personIds, Iterator<Long> postCreationDates, int limit) {
            this.personIds = personIds;
            this.postCreationDates = postCreationDates;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery2(personIds.next(), new Date(postCreationDates.next()), limit);
        }
    }

    class Query3Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIds;
        private final Iterator<String[]> countryPairs;
        private final Iterator<Long> startDates;
        private final long durationMillis;

        protected Query3Generator(Iterator<Long> personIds, Iterator<String[]> countryPairs,
                                  Iterator<Long> startDates, long durationDays) {
            this.personIds = personIds;
            this.countryPairs = countryPairs;
            this.startDates = startDates;
            this.durationMillis = durationDays;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            String[] countryPair = countryPairs.next();
            return new LdbcQuery3(personIds.next(), countryPair[0], countryPair[1], new Date(
                    startDates.next()), durationMillis);
        }
    }

    class Query4Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIds;
        private final Iterator<Long> postCreationDates;
        private final Iterator<Long> durationMillis;

        protected Query4Generator(Iterator<Long> personIds, Iterator<Long> postCreationDates, Iterator<Long> durationMillis) {
            this.personIds = personIds;
            this.postCreationDates = postCreationDates;
            this.durationMillis = durationMillis;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery4(personIds.next(), new Date(postCreationDates.next()), durationMillis.next());
        }
    }

    class Query5Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIds;
        private final Iterator<Long> joinDates;

        protected Query5Generator(Iterator<Long> personIds, Iterator<Long> joinDates) {
            this.personIds = personIds;
            this.joinDates = joinDates;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery5(personIds.next(), new Date(joinDates.next()));
        }
    }

    class Query6Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIds;
        private final Iterator<String> tagUris;
        private final int limit;

        protected Query6Generator(Iterator<Long> personIds, Iterator<String> tagUris, int limit) {
            this.personIds = personIds;
            this.tagUris = tagUris;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery6(personIds.next(), tagUris.next(), limit);
        }
    }

    class Query7Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIds;
        private final int limit;

        protected Query7Generator(Iterator<Long> personIds, int limit) {
            this.personIds = personIds;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery7(personIds.next(), limit);
        }
    }

    class Query8Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIds;
        private final int limit;

        protected Query8Generator(Iterator<Long> personIds, int limit) {
            this.personIds = personIds;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery8(personIds.next(), limit);
        }
    }

    class Query9Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIds;
        private final Iterator<Long> dates;
        private final int limit;

        protected Query9Generator(Iterator<Long> personIds, Iterator<Long> dates, int limit) {
            this.personIds = personIds;
            this.dates = dates;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery9(personIds.next(), dates.next(), limit);
        }
    }

    class Query10Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIds;
        private final Iterator<Integer> horoscopes;
        private final int limit;

        protected Query10Generator(Iterator<Long> personIds, Iterator<Integer> horoscopes, int limit) {
            this.personIds = personIds;
            this.horoscopes = horoscopes;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            int horoscopeSign1 = horoscopes.next();
            int horoscopeSign2 = (12 == horoscopeSign1) ? 1 : horoscopeSign1 + 1;
            return new LdbcQuery10(personIds.next(), horoscopeSign1, horoscopeSign2, limit);
        }
    }

    class Query11Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIds;
        private final Iterator<String> countries;
        private final Iterator<Integer> workFromYears;
        private final int limit;

        protected Query11Generator(Iterator<Long> personIds, Iterator<String> countries, Iterator<Integer> workFromYears, int limit) {
            this.personIds = personIds;
            this.countries = countries;
            this.workFromYears = workFromYears;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery11(personIds.next(), countries.next(), workFromYears.next(), limit);
        }
    }

    class Query12Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIds;
        private final Iterator<String> tagClasses;
        private final int limit;

        protected Query12Generator(Iterator<Long> personIds, Iterator<String> tagClasses, int limit) {
            this.personIds = personIds;
            this.tagClasses = tagClasses;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery12(personIds.next(), tagClasses.next(), limit);
        }
    }

    class Query13Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIds;

        protected Query13Generator(Iterator<Long> personIds) {
            this.personIds = personIds;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery13(personIds.next(), personIds.next());
        }
    }

    class Query14Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIds;

        protected Query14Generator(Iterator<Long> personIds) {
            this.personIds = personIds;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery14(personIds.next(), personIds.next());
        }
    }
}
