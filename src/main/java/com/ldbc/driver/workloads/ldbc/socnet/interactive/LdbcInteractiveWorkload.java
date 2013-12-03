package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import java.io.File;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.wrapper.StartTimeOperationGeneratorWrapper;
import com.ldbc.driver.util.GeneratorUtils;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple.Tuple2;
import com.ldbc.driver.util.temporal.Duration;
import com.ldbc.driver.util.temporal.Time;

/*
MAVEN_OPTS="-server -XX:+UseConcMarkSweepGC -Xmx512m" mvn exec:java -Dexec.mainClass=com.ldbc.driver.Client -Dexec.arguments="-db,com.ldbc.socialnet.workload.neo4j.Neo4jDb,-w,com.ldbc.socialnet.workload.LdbcInteractiveWorkload,-oc,10,-rc,-1,-tc,1,-s,-tu,MILLISECONDS,-p,neo4j.path=db/,-p,neo4j.dbtype=embedded-api-steps,-p,parameters=parameters.json"
 */
public class LdbcInteractiveWorkload extends Workload
{
    public static String PARAMETERS_FILENAME = "parameters";

    private SubstitutionParameters substitutionParameters = null;

    @Override
    public void onInit( Map<String, String> properties ) throws WorkloadException
    {
        // TODO make PARAMETERS_FILENAME compulsory
        String parametersFilename = properties.get( PARAMETERS_FILENAME );
        if ( false == new File( parametersFilename ).exists() )
        {
            throw new WorkloadException( "Substitution parameters file does not exist: " + parametersFilename );
        }
        File parametersFile = new File( parametersFilename );
        try
        {
            substitutionParameters = SubstitutionParameters.fromJson( parametersFile );
        }
        catch ( Exception e )
        {
            throw new WorkloadException( "Unable to load substitution parameters from " + parametersFile.getName() );
        }
    }

    @Override
    protected void onCleanup() throws WorkloadException
    {
    }

    @Override
    protected Iterator<Operation<?>> createLoadOperations( GeneratorFactory generatorBuilder ) throws WorkloadException
    {
        throw new UnsupportedOperationException( "Load phase not implemented for LDBC workload" );
    }

    @Override
    protected Iterator<Operation<?>> createTransactionalOperations( GeneratorFactory generators )
            throws WorkloadException
    {

        /*
         * Create Generators for desired Operations
         */

        Set<Tuple2<Double, Generator<Operation<?>>>> operations = new HashSet<Tuple2<Double, Generator<Operation<?>>>>();

        Generator<String> firstNameGenerator = generators.discreteGenerator( substitutionParameters.firstNames );
        Generator<Long> personIdGenerator = generators.discreteGenerator( substitutionParameters.personIds );
        Generator<Long> postCreationDateGenerator00_66 = generators.uniformNumberGenerator(
                substitutionParameters.postCreationDates.get( 0 ), substitutionParameters.postCreationDates.get( 66 ) );
        Generator<Long> postCreationDateGenerator33_66 = generators.uniformNumberGenerator(
                substitutionParameters.postCreationDates.get( 33 ), substitutionParameters.postCreationDates.get( 66 ) );
        Generator<Long> postCreationDateGenerator00_95 = generators.uniformNumberGenerator(
                substitutionParameters.postCreationDates.get( 0 ), substitutionParameters.postCreationDates.get( 95 ) );
        Generator<String[]> countryPairsGenerator = generators.discreteGenerator( substitutionParameters.countryPairs );
        Long postCreationDateRangeDuration100 = substitutionParameters.postCreationDates.get( 100 )
                                                - substitutionParameters.postCreationDates.get( 0 );
        Long postCreationDateRangeDuration002 = substitutionParameters.postCreationDates.get( 2 )
                                                - substitutionParameters.postCreationDates.get( 0 );
        Long postCreationDateRangeDuration004 = substitutionParameters.postCreationDates.get( 4 )
                                                - substitutionParameters.postCreationDates.get( 0 );
        Generator<Long> postCreationDateRangeDuration02_04 = generators.uniformNumberGenerator(
                postCreationDateRangeDuration002, postCreationDateRangeDuration004 );
        Generator<String> tagUriGenerator = generators.discreteGenerator( substitutionParameters.tagUris );

        /*
         * Query1
         *  - Select uniformly randomly from person first names
         */
        int query1Limit = 10;
        operations.add( Tuple.tuple2( 1d, (Generator<Operation<?>>) new Query1Generator( firstNameGenerator,
                query1Limit ) ) );

        /*
         * Query2
         *  - Person ID - select uniformly randomly from person ids
         *  - Post Creation Date - select uniformly randomly a post creation date from between 33perc-66perc of entire date range
         */
        int query2Limit = 20;
        operations.add( Tuple.tuple2( 1d, (Generator<Operation<?>>) new Query2Generator( personIdGenerator,
                postCreationDateGenerator33_66, query2Limit ) ) );

        /*
         * Query3
         *  - Person ID - select uniformly randomly from person ids
         *  - Post Creation Date - select uniformly randomly a post creation date from between 0perc-66perc of entire date range
         *  - Duration - a number of days (33% of the length of post creation date range)
         *  - Country1 - the first of country pair (file: countryPairs.txt)
         *  - Country2 - the second of country pair (file: countryPairs.txt)
         */
        operations.add( Tuple.tuple2( 1d, (Generator<Operation<?>>) new Query3Generator( personIdGenerator,
                countryPairsGenerator, postCreationDateGenerator00_66, postCreationDateRangeDuration100 / 3 ) ) );

        /*
         * Query4
         * - Person ID - select uniformly randomly from person ids
         * - Post Creation Date - select uniformly randomly a post creation date from between 0perc-95perc of entire date range
         * - Duration - a uniformly randomly selected duration between 2% and 4% of the length of post creation date range        
         */
        operations.add( Tuple.tuple2( 1d, (Generator<Operation<?>>) new Query4Generator( personIdGenerator,
                postCreationDateGenerator00_95, postCreationDateRangeDuration02_04 ) ) );

        /*
         * Query5
         * - Person - select uniformly randomly from person ids
         * - Join Date - select uniformly randomly a post creation date from between 0perc-95perc of entire date range
         * TODO (remove from Confluence Sub Params page) - Duration - a uniformly randomly selected duration between 2% and 4% of the length of post creation date range
         */
        operations.add( Tuple.tuple2( 1d, (Generator<Operation<?>>) new Query5Generator( personIdGenerator,
                postCreationDateGenerator00_95 ) ) );

        /*
         * Query6
         * - Person - select uniformly randomly from person ids
         * - Tag - select uniformly randomly from tag uris
         */
        int query6Limit = 10;
        operations.add( Tuple.tuple2( 1d, (Generator<Operation<?>>) new Query6Generator( personIdGenerator,
                tagUriGenerator, query6Limit ) ) );

        /*
         * Query 7
         * TODO - Person - select uniformly randomly from person ids
         * TODO date
         * TODO duration
         */
        // TODO is limit 10?
        int query7Limit = 10;
        // TODO Sub Params Confluence doesn't describe date sub policy
        // TODO Sub Params Confluence doesn't describe duration sub policy
        operations.add( Tuple.tuple2( 1d, (Generator<Operation<?>>) new Query7Generator( personIdGenerator,
                postCreationDateGenerator00_95, postCreationDateRangeDuration02_04, query7Limit ) ) );

        /*
         * Create Discrete Generator from 
         */

        Generator<Operation<?>> operationGenerator = generators.weightedDiscreteDereferencingGenerator( operations );

        /*
         * Filter Interesting Operations
         */

        final Set<Class<? extends Operation<?>>> operationsToInclude = new HashSet<Class<? extends Operation<?>>>();
        operationsToInclude.add( LdbcQuery1.class );
        operationsToInclude.add( LdbcQuery3.class );
        operationsToInclude.add( LdbcQuery4.class );
        operationsToInclude.add( LdbcQuery5.class );
        operationsToInclude.add( LdbcQuery6.class );
        operationsToInclude.add( LdbcQuery7.class );

        Predicate<Operation<?>> allowedOperationsFilter = new Predicate<Operation<?>>()
        {
            @Override
            public boolean apply( Operation<?> input )
            {
                return operationsToInclude.contains( input.getClass() );
            }
        };

        Iterator<Operation<?>> filteredGenerator = Iterators.filter( operationGenerator, allowedOperationsFilter );

        // Generator<Time> startTimeGenerator =
        // GeneratorUtils.randomTimeGeneratorFromNow( generatorBuilder,
        // Time.now(),
        // Time.fromMilli( 100 ).asMilli(), Time.fromMilli( 1000 ).asMilli() );

        Generator<Time> startTimeGenerator = GeneratorUtils.constantTimeGeneratorFromNow( generators, Time.now(),
                Duration.fromMilli( 100 ) );

        return new StartTimeOperationGeneratorWrapper( startTimeGenerator, filteredGenerator );
    }

    class Query1Generator extends Generator<Operation<?>>
    {
        private final Generator<String> firstNames;
        private final int limit;

        protected Query1Generator( Generator<String> firstNames, int limit )
        {
            this.firstNames = firstNames;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException
        {
            return new LdbcQuery1( firstNames.next(), limit );
        }
    }

    class Query2Generator extends Generator<Operation<?>>
    {
        private final Generator<Long> personIds;
        private final Generator<Long> postCreationDates;
        private final int limit;

        protected Query2Generator( Generator<Long> personIds, Generator<Long> postCreationDates, int limit )
        {
            this.personIds = personIds;
            this.postCreationDates = postCreationDates;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException
        {
            return new LdbcQuery2( personIds.next(), new Date( postCreationDates.next() ), limit );
        }
    }

    class Query3Generator extends Generator<Operation<?>>
    {
        private final Generator<Long> personIds;
        private final Generator<String[]> countryPairs;
        private final Generator<Long> startDates;
        private final long durationMillis;

        protected Query3Generator( Generator<Long> personIds, Generator<String[]> countryPairs,
                Generator<Long> startDates, long durationDays )
        {
            this.personIds = personIds;
            this.countryPairs = countryPairs;
            this.startDates = startDates;
            this.durationMillis = durationDays;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException
        {
            return new LdbcQuery3( personIds.next(), countryPairs.next()[0], countryPairs.next()[1], new Date(
                    startDates.next() ), durationMillis );
        }
    }

    class Query4Generator extends Generator<Operation<?>>
    {
        private final Generator<Long> personIds;
        private final Generator<Long> postCreationDates;
        private final Generator<Long> durationMillis;

        protected Query4Generator( Generator<Long> personIds, Generator<Long> postCreationDates,
                Generator<Long> durationMillis )
        {
            this.personIds = personIds;
            this.postCreationDates = postCreationDates;
            this.durationMillis = durationMillis;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException
        {
            return new LdbcQuery4( personIds.next(), new Date( postCreationDates.next() ), durationMillis.next() );
        }
    }

    class Query5Generator extends Generator<Operation<?>>
    {
        private final Generator<Long> personIdGenerator;
        private final Generator<Long> joinDates;

        protected Query5Generator( Generator<Long> personIdGenerator, Generator<Long> joinDates )
        {
            this.personIdGenerator = personIdGenerator;
            this.joinDates = joinDates;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException
        {
            return new LdbcQuery5( personIdGenerator.next(), new Date( joinDates.next() ) );
        }
    }

    class Query6Generator extends Generator<Operation<?>>
    {
        private final Generator<Long> personIdGenerator;
        private final Generator<String> tagUriGenerator;
        private final int limit;

        protected Query6Generator( Generator<Long> personIdGenerator, Generator<String> tagUriGenerator, int limit )
        {
            this.personIdGenerator = personIdGenerator;
            this.tagUriGenerator = tagUriGenerator;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException
        {
            return new LdbcQuery6( personIdGenerator.next(), tagUriGenerator.next(), limit );
        }
    }

    class Query7Generator extends Generator<Operation<?>>
    {
        private final Generator<Long> personIdGenerator;
        private final Generator<Long> endDateGenerator;
        private final Generator<Long> durationMillisGenerator;
        private final int limit;

        protected Query7Generator( Generator<Long> personIdGenerator, Generator<Long> endDateGenerator,
                Generator<Long> durationMillisGenerator, int limit )
        {
            this.personIdGenerator = personIdGenerator;
            this.endDateGenerator = endDateGenerator;
            this.durationMillisGenerator = durationMillisGenerator;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException
        {
            return new LdbcQuery7( personIdGenerator.next(), new Date( endDateGenerator.next() ),
                    durationMillisGenerator.next(), limit );
        }
    }
}
