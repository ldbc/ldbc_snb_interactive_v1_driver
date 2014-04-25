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
    public final static String UPDATE_STREAM_FILENAME_KEY = "updates";
    public final static String PARAMETERS_FILENAME_KEY = "parameters";
    public final static String INTERLEAVE_DURATION_KEY = "interleave_duration";
    public final static String READ_RATIO_KEY = "read_ratio";
    public final static String WRITE_RATIO_KEY = "write_ratio";
    public final static String QUERY_1_KEY = LdbcQuery1.class.getName();
    public final static String QUERY_2_KEY = LdbcQuery2.class.getName();
    public final static String QUERY_3_KEY = LdbcQuery3.class.getName();
    public final static String QUERY_4_KEY = LdbcQuery4.class.getName();
    public final static String QUERY_5_KEY = LdbcQuery5.class.getName();
    public final static String QUERY_6_KEY = LdbcQuery6.class.getName();
    public final static String QUERY_7_KEY = LdbcQuery7.class.getName();
    public final static String QUERY_8_KEY = LdbcQuery8.class.getName();
    public final static String QUERY_9_KEY = LdbcQuery9.class.getName();
    public final static String QUERY_10_KEY = LdbcQuery10.class.getName();
    public final static String QUERY_11_KEY = LdbcQuery11.class.getName();
    public final static String QUERY_12_KEY = LdbcQuery12.class.getName();
    public final static List<String> QUERY_KEYS = Lists.newArrayList(
            QUERY_1_KEY,
            QUERY_2_KEY,
            QUERY_3_KEY,
            QUERY_4_KEY,
            QUERY_5_KEY,
            QUERY_6_KEY,
            QUERY_7_KEY,
            QUERY_8_KEY,
            QUERY_9_KEY,
            QUERY_10_KEY,
            QUERY_11_KEY,
            QUERY_12_KEY);

    private UpdateEventStreamReader updateOperations;
    private SubstitutionParameters substitutionParameters;
    private Duration interleaveDuration;
    private double readRatio;
    private double writeRatio;
    private Map<Class, Double> queryMix;

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
        return operationClassifications;
    }

    @Override
    public void onInit(Map<String, String> properties) throws WorkloadException {
        List<String> compulsoryKeys = Lists.newArrayList(
                UPDATE_STREAM_FILENAME_KEY,
                PARAMETERS_FILENAME_KEY,
                INTERLEAVE_DURATION_KEY,
                READ_RATIO_KEY,
                WRITE_RATIO_KEY);
        compulsoryKeys.addAll(QUERY_KEYS);

        List<String> missingPropertyParameters = missingPropertiesParameters(properties, compulsoryKeys);
        if (false == missingPropertyParameters.isEmpty())
            throw new WorkloadException(String.format("Workload could not initialize due to missing parameters: %s", missingPropertyParameters.toString()));

        String updatesFilename = properties.get(UPDATE_STREAM_FILENAME_KEY);
        if (false == new File(updatesFilename).exists()) {
            throw new WorkloadException(String.format("Update events file does not exist: %s", updatesFilename));
        }
        File updatesFile = new File(updatesFilename);
        try {
            updateOperations = new UpdateEventStreamReader(updatesFile);
        } catch (Exception e) {
            throw new WorkloadException(String.format("Unable to load update event stream from: %s", updatesFile.getAbsolutePath()), e);
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

        queryMix = new HashMap<>();
        for (String queryKey : QUERY_KEYS) {
            String queryRatioString = properties.get(queryKey);
            Double queryRatio = Double.parseDouble(queryRatioString);
            try {
                Class queryClass = ClassLoaderHelper.loadClass(queryKey);
                queryMix.put(queryClass, queryRatio);
            } catch (ClassLoadingException e) {
                throw new WorkloadException(String.format("Unable to load query class: %s", queryKey), e.getCause());
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
        c.clear();
        c.set(Calendar.YEAR, substitutionParameters.minWorkFrom);
        long workFromDateAsMs000 = c.getTimeInMillis();
        c.clear();
        c.set(Calendar.YEAR, substitutionParameters.maxWorkFrom);
        long workFromDateAsMs100 = c.getTimeInMillis();
        Iterator<Long> workFromDateGenerator00_100 = generators.uniform(workFromDateAsMs000, workFromDateAsMs100);
        Iterator<String> tagClassesGenerator = generators.discrete(substitutionParameters.tagClasses);

        /*
         * Create Generators for desired Operations
         */
        List<Tuple2<Double, Iterator<Operation<?>>>> operationsMix = new ArrayList<>();

        /*
         * Query1
         *  - Select uniformly randomly from person first names
         */
        int query1Limit = LdbcQuery1.DEFAULT_LIMIT;
        operationsMix.add(Tuple.tuple2(
                queryMix.get(LdbcQuery1.class),
                (Iterator<Operation<?>>) new Query1Generator(firstNameGenerator, query1Limit)));

        /*
         * Query2
         *  - Person ID - select uniformly randomly from person ids
         *  - Post Creation Date - select uniformly randomly a post creation date from between 33perc-66perc of entire date range
         */
        int query2Limit = LdbcQuery2.DEFAULT_LIMIT;
        operationsMix.add(Tuple.tuple2(
                queryMix.get(LdbcQuery2.class),
                (Iterator<Operation<?>>) new Query2Generator(personIdGenerator, postCreationDateGenerator33_66, query2Limit)));

        /*
         * Query3
         *  - Person ID - select uniformly randomly from person ids
         *  - Post Creation Date - select uniformly randomly a post creation date from between 0perc-66perc of entire date range
         *  - Duration - a number of days (33% of the length of post creation date range)
         *  - Country1 - the first of country pair
         *  - Country2 - the second of country pair
         */
        operationsMix.add(Tuple.tuple2(
                queryMix.get(LdbcQuery3.class),
                (Iterator<Operation<?>>) new Query3Generator(personIdGenerator, countryPairsGenerator, postCreationDateGenerator00_66, postCreationDateRangeAsMs033)));

        /*
         * Query4
         * - Person ID - select uniformly randomly from person ids
         * - Post Creation Date - select uniformly randomly a post creation date from between 0perc-95perc of entire date range
         * - Duration - a uniformly randomly selected duration between 2% and 4% of the length of post creation date range        
         */
        operationsMix.add(Tuple.tuple2(
                queryMix.get(LdbcQuery4.class),
                (Iterator<Operation<?>>) new Query4Generator(personIdGenerator, postCreationDateGenerator00_95, postCreationDateRangeDuration02_04)));

        // TODO http://www.ldbc.eu:8090/display/TUC/IW+Substitution+parameters+selection claims Q5 needs duration parameter
        // TODO http://www.ldbc.eu:8090/display/TUC/Interactive+Workload does not show that parameters
        // TODO add duration to operation if it found that it's necessary
        /*
         * Query5
         * - Person - select uniformly randomly from person ids
         * - Join Date - select uniformly randomly a post creation date from between 0perc-95perc of entire date range
         */
        operationsMix.add(Tuple.tuple2(
                queryMix.get(LdbcQuery5.class),
                (Iterator<Operation<?>>) new Query5Generator(personIdGenerator, postCreationDateGenerator00_95)));

        /*
         * Query6
         * - Person - select uniformly randomly from person ids
         * - Tag - select uniformly randomly from tag uris
         */
        int query6Limit = LdbcQuery6.DEFAULT_LIMIT;
        operationsMix.add(Tuple.tuple2(
                queryMix.get(LdbcQuery6.class),
                (Iterator<Operation<?>>) new Query6Generator(personIdGenerator, tagNameGenerator, query6Limit)));

        /*
         * Query 7
         * Person - select uniformly randomly from person ids
         */
        int query7Limit = LdbcQuery7.DEFAULT_LIMIT;
        operationsMix.add(Tuple.tuple2(
                queryMix.get(LdbcQuery7.class),
                (Iterator<Operation<?>>) new Query7Generator(personIdGenerator, query7Limit)));

        /*
         * Query 8
         * Person - select uniformly randomly from person ids
         */
        int query8Limit = LdbcQuery8.DEFAULT_LIMIT;
        operationsMix.add(Tuple.tuple2(
                queryMix.get(LdbcQuery8.class),
                (Iterator<Operation<?>>) new Query8Generator(personIdGenerator, query8Limit)));

        /*
         * Query 9
         * Person - select uniformly randomly from person ids
         * Date - select uniformly randomly a post creation date from between 33perc-66perc of entire date range
         */
        int query9Limit = LdbcQuery9.DEFAULT_LIMIT;
        operationsMix.add(Tuple.tuple2(
                queryMix.get(LdbcQuery9.class),
                (Iterator<Operation<?>>) new Query9Generator(personIdGenerator, postCreationDateGenerator33_66, query9Limit)));

        /*
         * Query 10
         * Person - select uniformly randomly from person ids
         * HS0 - select uniformly randomly a horoscope sign (a random number between 1 and 12)
         * HS1 - HS0 + 1 (but 12 + 1 = 1)
         */
        int query10Limit = LdbcQuery10.DEFAULT_LIMIT;
        operationsMix.add(Tuple.tuple2(
                queryMix.get(LdbcQuery10.class),
                (Iterator<Operation<?>>) new Query10Generator(personIdGenerator, horoscopeGenerator, query10Limit)));

        /*
         * Query 11
         * Person - select uniformly randomly from person ids
         * // TODO parameters dont have IDS only names
         * Country - select uniformly randomly from country ids
         * Date - a random date from 0% to 100% of whole workFrom timeline
         */
        int query11Limit = LdbcQuery11.DEFAULT_LIMIT;
        operationsMix.add(Tuple.tuple2(
                queryMix.get(LdbcQuery11.class),
                (Iterator<Operation<?>>) new Query11Generator(personIdGenerator, countriesGenerator, workFromDateGenerator00_100, query11Limit)));

        /*
         * Query 12
         * Person - select uniformly randomly from person ids
         * TagType - select uniformly randomly tagTypeURI (used files: tagTypes.txt and tagTypes.sql)
         */
        int query12Limit = LdbcQuery12.DEFAULT_LIMIT;
        operationsMix.add(Tuple.tuple2(
                queryMix.get(LdbcQuery12.class),
                (Iterator<Operation<?>>) new Query12Generator(personIdGenerator, tagClassesGenerator, query12Limit)));

        /*
         * Create Discrete Generator from query mix
         */
        Iterator<Operation<?>> operationGenerator = generators.weightedDiscreteDereferencing(operationsMix);

        /*
         * Filter Interesting Operations
         */
        final Set<Class> operationsToInclude = queryMix.keySet();
        Predicate<Operation<?>> allowedOperationsFilter = new Predicate<Operation<?>>() {
            @Override
            public boolean apply(Operation<?> query) {
                return operationsToInclude.contains(query.getClass());
            }
        };

        Iterator<Operation<?>> interactiveReadOperations = Iterators.filter(operationGenerator, allowedOperationsFilter);

        // TODO test if interleave actually works correctly
        Iterator<Time> startTimeGenerator = GeneratorUtils.constantIncrementStartTimeGenerator(generators, Time.now(), interleaveDuration);

        Iterator<Operation<?>> interactiveReadOperationsWithTime = new StartTimeAssigningOperationGenerator(startTimeGenerator, interactiveReadOperations);

        // TODO use readRatio/writeRatio & uncomment
        // Iterator<Operation<?>> interactiveAndUpdateQueries_INTERLEAVED = generators.interleave(interactiveReadOperations,updateOperations,10);

        // TODO uncomment
        // List<Tuple2<Double, Iterator<Operation<?>>>> readWriteOperationMix = new ArrayList<>();
        // readWriteOperationMix.add(Tuple.tuple2(readRatio, interactiveReadOperationsWithTime));
        // readWriteOperationMix.add(Tuple.tuple2(writeRatio, (Iterator<Operation<?>>) updateOperations));
        // Iterator<Operation<?>> interactiveAndUpdateQueries_MIXED = generators.weightedDiscreteDereferencing(readWriteOperationMix);

        return interactiveReadOperationsWithTime;
    }

    class Query1Generator extends Generator<Operation<?>> {
        private final Iterator<String> firstNames;
        private final int limit;

        protected Query1Generator(Iterator<String> firstNames, int limit) {
            this.firstNames = firstNames;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery1(firstNames.next(), limit);
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
        private final Iterator<Long> personIdGenerator;
        private final Iterator<Long> joinDates;

        protected Query5Generator(Iterator<Long> personIdGenerator, Iterator<Long> joinDates) {
            this.personIdGenerator = personIdGenerator;
            this.joinDates = joinDates;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery5(personIdGenerator.next(), new Date(joinDates.next()));
        }
    }

    class Query6Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIdGenerator;
        private final Iterator<String> tagUriGenerator;
        private final int limit;

        protected Query6Generator(Iterator<Long> personIdGenerator, Iterator<String> tagUriGenerator, int limit) {
            this.personIdGenerator = personIdGenerator;
            this.tagUriGenerator = tagUriGenerator;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery6(personIdGenerator.next(), tagUriGenerator.next(), limit);
        }
    }

    class Query7Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIdGenerator;
        private final int limit;

        protected Query7Generator(Iterator<Long> personIdGenerator, int limit) {
            this.personIdGenerator = personIdGenerator;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery7(personIdGenerator.next(), limit);
        }
    }

    class Query8Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIdGenerator;
        private final int limit;

        protected Query8Generator(Iterator<Long> personIdGenerator, int limit) {
            this.personIdGenerator = personIdGenerator;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery8(personIdGenerator.next(), limit);
        }
    }

    class Query9Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIdGenerator;
        private final Iterator<Long> dateGenerator;
        private final int limit;

        protected Query9Generator(Iterator<Long> personIdGenerator, Iterator<Long> dateGenerator, int limit) {
            this.personIdGenerator = personIdGenerator;
            this.dateGenerator = dateGenerator;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery9(personIdGenerator.next(), dateGenerator.next(), limit);
        }
    }

    class Query10Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIdGenerator;
        private final Iterator<Integer> horoscopeGenerator;
        private final int limit;

        protected Query10Generator(Iterator<Long> personIdGenerator, Iterator<Integer> horoscopeGenerator, int limit) {
            this.personIdGenerator = personIdGenerator;
            this.horoscopeGenerator = horoscopeGenerator;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            int horoscopeSign1 = horoscopeGenerator.next();
            int horoscopeSign2 = (12 == horoscopeSign1) ? 1 : horoscopeSign1 + 1;
            return new LdbcQuery10(personIdGenerator.next(), horoscopeSign1, horoscopeSign2, limit);
        }
    }

    class Query11Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIdGenerator;
        private final Iterator<String> countriesGenerator;
        private final Iterator<Long> workFromDateGenerator;
        private final int limit;

        protected Query11Generator(Iterator<Long> personIdGenerator, Iterator<String> countriesGenerator, Iterator<Long> workFromDateGenerator, int limit) {
            this.personIdGenerator = personIdGenerator;
            this.countriesGenerator = countriesGenerator;
            this.workFromDateGenerator = workFromDateGenerator;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery11(personIdGenerator.next(), countriesGenerator.next(), workFromDateGenerator.next(), limit);
        }
    }

    class Query12Generator extends Generator<Operation<?>> {
        private final Iterator<Long> personIdGenerator;
        private final Iterator<String> tagClassesGenerator;
        private final int limit;

        protected Query12Generator(Iterator<Long> personIdGenerator, Iterator<String> tagClassesGenerator, int limit) {
            this.personIdGenerator = personIdGenerator;
            this.tagClassesGenerator = tagClassesGenerator;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery12(personIdGenerator.next(), tagClassesGenerator.next(), limit);
        }
    }
}
