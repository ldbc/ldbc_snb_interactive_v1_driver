package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.StartTimeAssigningOperationGenerator;
import com.ldbc.driver.runtime.streams.OperationClassification;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.ClassLoadingException;
import com.ldbc.driver.util.GeneratorUtils;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple.Tuple2;

import java.io.File;
import java.util.*;

public class LdbcInteractiveWorkload extends Workload {
    public final static String PARAMETERS_FILENAME_KEY = "parameters";
    public final static String INTERLEAVE_DURATION_KEY = "interleave_duration";
    public final static String QUERY_1_KEY = LdbcQuery1.class.getName();
    public final static String QUERY_2_KEY = LdbcQuery2.class.getName();
    public final static String QUERY_3_KEY = LdbcQuery3.class.getName();
    public final static String QUERY_4_KEY = LdbcQuery4.class.getName();
    public final static String QUERY_5_KEY = LdbcQuery5.class.getName();
    public final static String QUERY_6_KEY = LdbcQuery6.class.getName();
    public final static String QUERY_7_KEY = LdbcQuery7.class.getName();
    public final static List<String> QUERY_KEYS = Lists.newArrayList(
            QUERY_1_KEY,
            QUERY_2_KEY,
            QUERY_3_KEY,
            QUERY_4_KEY,
            QUERY_5_KEY,
            QUERY_6_KEY,
            QUERY_7_KEY);

    private SubstitutionParameters substitutionParameters = null;
    private Duration interleaveDuration = null;
    private Map<Class, Double> queryMix = null;

    @Override
    protected Map<Class<?>, OperationClassification> operationClassificationMapping() {
        Map<Class<?>, OperationClassification> operationClassificationMapping = new HashMap<Class<?>, OperationClassification>();
        // TODO use correct operation classifications
        // TODO need to add new classification to support: no window, no gct, identity scheduling - i.e., only policy driver used to support
        operationClassificationMapping.put(LdbcQuery1.class, OperationClassification.WindowFalse_GCTRead);
        operationClassificationMapping.put(LdbcQuery2.class, OperationClassification.WindowFalse_GCTReadWrite);
        operationClassificationMapping.put(LdbcQuery3.class, OperationClassification.WindowTrue_GCTRead);
        operationClassificationMapping.put(LdbcQuery4.class, OperationClassification.WindowTrue_GCTReadWrite);
        operationClassificationMapping.put(LdbcQuery5.class, OperationClassification.WindowFalse_GCTRead);
        operationClassificationMapping.put(LdbcQuery6.class, OperationClassification.WindowFalse_GCTReadWrite);
        operationClassificationMapping.put(LdbcQuery7.class, OperationClassification.WindowTrue_GCTRead);
        return operationClassificationMapping;
    }

    @Override
    public void onInit(Map<String, String> properties) throws WorkloadException {
        List<String> compulsoryKeys = Lists.newArrayList(PARAMETERS_FILENAME_KEY, INTERLEAVE_DURATION_KEY);
        compulsoryKeys.addAll(QUERY_KEYS);

        List<String> missingProperteryParameters = missingPropertiesParameters(properties, compulsoryKeys);
        if (false == missingProperteryParameters.isEmpty())
            throw new WorkloadException(
                    String.format(
                            "Workload could not initialize due to missing parameters: %s",
                            missingProperteryParameters.toString()));

        String parametersFilename = properties.get(PARAMETERS_FILENAME_KEY);
        if (false == new File(parametersFilename).exists()) {
            throw new WorkloadException(
                    String.format(
                            "Substitution parameters file does not exist: %s",
                            parametersFilename));
        }
        File parametersFile = new File(parametersFilename);
        try {
            substitutionParameters = SubstitutionParameters.fromJson(parametersFile);
        } catch (Exception e) {
            throw new WorkloadException(
                    String.format(
                            "Unable to load substitution parameters from: %s",
                            parametersFile.getAbsolutePath()),
                    e.getCause());
        }

        try {
            long interleaveDurationMs = Long.parseLong(properties.get(INTERLEAVE_DURATION_KEY));
            interleaveDuration = Duration.fromMilli(interleaveDurationMs);
        } catch (NumberFormatException e) {
            throw new WorkloadException(
                    String.format(
                            "Unable to parse interleave duration: %s",
                            properties.get(INTERLEAVE_DURATION_KEY)),
                    e.getCause());
        }

        queryMix = new HashMap<Class, Double>();
        for (String queryKey : QUERY_KEYS) {
            String queryRatioString = properties.get(queryKey);
            Double queryRatio = Double.parseDouble(queryRatioString);
            try {
                Class queryClass = ClassLoaderHelper.loadClass(queryKey);
                queryMix.put(queryClass, queryRatio);
            } catch (ClassLoadingException e) {
                throw new WorkloadException(
                        String.format("Unable to load query class: %s", queryKey), e.getCause());
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
    protected Iterator<Operation<?>> createOperations(GeneratorFactory generators)
            throws WorkloadException {

        /*
         * Create Generators for desired Operations
         */

        List<Tuple2<Double, Iterator<Operation<?>>>> operations = new ArrayList<Tuple2<Double, Iterator<Operation<?>>>>();

        Iterator<String> firstNameGenerator = generators.discrete(substitutionParameters.firstNames);
        Iterator<Long> personIdGenerator = generators.discrete(substitutionParameters.personIds);
        Iterator<Long> postCreationDateGenerator00_66 = generators.uniform(
                substitutionParameters.postCreationDates.get(0), substitutionParameters.postCreationDates.get(66));
        Iterator<Long> postCreationDateGenerator33_66 = generators.uniform(
                substitutionParameters.postCreationDates.get(33), substitutionParameters.postCreationDates.get(66));
        Iterator<Long> postCreationDateGenerator00_95 = generators.uniform(
                substitutionParameters.postCreationDates.get(0), substitutionParameters.postCreationDates.get(95));
        Iterator<String[]> countryPairsGenerator = generators.discrete(substitutionParameters.countryPairs);
        Long postCreationDateRangeDuration100 = substitutionParameters.postCreationDates.get(100)
                - substitutionParameters.postCreationDates.get(0);
        Long postCreationDateRangeDuration002 = substitutionParameters.postCreationDates.get(2)
                - substitutionParameters.postCreationDates.get(0);
        Long postCreationDateRangeDuration004 = substitutionParameters.postCreationDates.get(4)
                - substitutionParameters.postCreationDates.get(0);
        Iterator<Long> postCreationDateRangeDuration02_04 = generators.uniform(postCreationDateRangeDuration002,
                postCreationDateRangeDuration004);
        Iterator<String> tagUriGenerator = generators.discrete(substitutionParameters.tagUris);

        /*
         * Query1
         *  - Select uniformly randomly from person first names
         */
        int query1Limit = 10;
        operations.add(Tuple.tuple2(queryMix.get(LdbcQuery1.class),
                (Iterator<Operation<?>>) new Query1Generator(firstNameGenerator, query1Limit)));

        /*
         * Query2
         *  - Person ID - select uniformly randomly from person ids
         *  - Post Creation Date - select uniformly randomly a post creation date from between 33perc-66perc of entire date range
         */
        int query2Limit = 20;
        operations.add(Tuple.tuple2(queryMix.get(LdbcQuery2.class), (Iterator<Operation<?>>) new Query2Generator(personIdGenerator,
                postCreationDateGenerator33_66, query2Limit)));

        /*
         * Query3
         *  - Person ID - select uniformly randomly from person ids
         *  - Post Creation Date - select uniformly randomly a post creation date from between 0perc-66perc of entire date range
         *  - Duration - a number of days (33% of the length of post creation date range)
         *  - Country1 - the first of country pair (file: countryPairs.txt)
         *  - Country2 - the second of country pair (file: countryPairs.txt)
         */
        operations.add(Tuple.tuple2(queryMix.get(LdbcQuery3.class), (Iterator<Operation<?>>) new Query3Generator(personIdGenerator,
                countryPairsGenerator, postCreationDateGenerator00_66, postCreationDateRangeDuration100 / 3)));

        /*
         * Query4
         * - Person ID - select uniformly randomly from person ids
         * - Post Creation Date - select uniformly randomly a post creation date from between 0perc-95perc of entire date range
         * - Duration - a uniformly randomly selected duration between 2% and 4% of the length of post creation date range        
         */
        operations.add(Tuple.tuple2(queryMix.get(LdbcQuery4.class), (Iterator<Operation<?>>) new Query4Generator(personIdGenerator,
                postCreationDateGenerator00_95, postCreationDateRangeDuration02_04)));

        /*
         * Query5
         * - Person - select uniformly randomly from person ids
         * - Join Date - select uniformly randomly a post creation date from between 0perc-95perc of entire date range
         */
        operations.add(Tuple.tuple2(queryMix.get(LdbcQuery5.class), (Iterator<Operation<?>>) new Query5Generator(personIdGenerator,
                postCreationDateGenerator00_95)));

        /*
         * Query6
         * - Person - select uniformly randomly from person ids
         * - Tag - select uniformly randomly from tag uris
         */
        int query6Limit = 10;
        operations.add(Tuple.tuple2(queryMix.get(LdbcQuery6.class), (Iterator<Operation<?>>) new Query6Generator(personIdGenerator,
                tagUriGenerator, query6Limit)));

        /*
         * Query 7
         * Person - select uniformly randomly from person ids
         * TODO (comment here) date
         * TODO (comment here) duration
         */
        // TODO is limit 10?
        int query7Limit = 10;
        operations.add(Tuple.tuple2(queryMix.get(LdbcQuery7.class), (Iterator<Operation<?>>) new Query7Generator(personIdGenerator,
                postCreationDateGenerator00_95, postCreationDateRangeDuration02_04, query7Limit)));

        /*
         * Create Discrete Generator from 
         */

        Iterator<Operation<?>> operationGenerator = generators.weightedDiscreteDereferencing(operations);

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

        Iterator<Operation<?>> filteredGenerator = Iterators.filter(operationGenerator, allowedOperationsFilter);

        // TODO test if interleave actually works correctly
        Iterator<Time> startTimeGenerator = GeneratorUtils.constantIncrementStartTimeGenerator(generators, Time.now(),
                interleaveDuration);

        return new StartTimeAssigningOperationGenerator(startTimeGenerator, filteredGenerator);
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

        protected Query4Generator(Iterator<Long> personIds, Iterator<Long> postCreationDates,
                                  Iterator<Long> durationMillis) {
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
        private final Iterator<Long> endDateGenerator;
        private final Iterator<Long> durationMillisGenerator;
        private final int limit;

        protected Query7Generator(Iterator<Long> personIdGenerator, Iterator<Long> endDateGenerator,
                                  Iterator<Long> durationMillisGenerator, int limit) {
            this.personIdGenerator = personIdGenerator;
            this.endDateGenerator = endDateGenerator;
            this.durationMillisGenerator = durationMillisGenerator;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException {
            return new LdbcQuery7(personIdGenerator.next(), new Date(endDateGenerator.next()),
                    durationMillisGenerator.next(), limit);
        }
    }
}
