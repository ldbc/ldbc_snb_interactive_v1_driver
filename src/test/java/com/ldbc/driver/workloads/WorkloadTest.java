package com.ldbc.driver.workloads;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.Client;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.client.ClientMode;
import com.ldbc.driver.client.ValidateDatabaseMode;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.ControlService;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.control.Log4jLoggingServiceFactory;
import com.ldbc.driver.csv.simple.SimpleCsvFileReader;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedMetricsService;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.util.Tuple2;
import com.ldbc.driver.validation.ClassNameWorkloadFactory;
import com.ldbc.driver.validation.DbValidationResult;
import com.ldbc.driver.validation.WorkloadFactory;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public abstract class WorkloadTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    TimeSource timeSource = new SystemTimeSource();

    public abstract Workload workload() throws Exception;

    public abstract List<Operation> operations() throws Exception;

    public abstract List<DriverConfiguration> configurations() throws Exception;

    public abstract List<Tuple2<DriverConfiguration,Histogram<Class,Double>>> configurationsWithExpectedQueryMix()
            throws Exception;

    @Test
    public void shouldBeAbleToSerializeAndMarshalAllOperations() throws Exception
    {
        // Given
        try ( Workload workload = workload() )
        {
            List<Operation> operations = operations();

            // When
            List<String> serializedOperations = new ArrayList<>();
            for ( Operation operation : operations )
            {
                serializedOperations.add( workload.serializeOperation( operation ) );
            }

            // Then
            for ( int i = 0; i < serializedOperations.size(); i++ )
            {
                String serializedOperation = serializedOperations.get( i );
                assertThat( workload.marshalOperation( serializedOperation ), equalTo( operations.get( i ) ) );
            }
        }
    }

    @Test
    public void shouldGenerateManyOperationsInReasonableTimeForLongReadOnly() throws Exception
    {
        for ( DriverConfiguration configuration : configurations() )
        {
            long operationCount = 1_000_000;
            long timeoutAsMilli = TimeUnit.SECONDS.toMillis( 5 );

            try ( Workload workload = new ClassNameWorkloadFactory( configuration.workloadClassName() ).createWorkload() )
            {
                workload.init( configuration );
                GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42L ) );
                Iterator<Operation> operations = gf.limit(
                        WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(
                                gf,
                                workload.streams( gf, true )
                        ),
                        operationCount
                );
                long timeout = timeSource.nowAsMilli() + timeoutAsMilli;
                boolean workloadGeneratedOperationsBeforeTimeout =
                        TestUtils.generateBeforeTimeout( operations, timeout, timeSource, operationCount );
                assertTrue( workloadGeneratedOperationsBeforeTimeout );
            }
        }
    }

    @Test
    public void shouldBeRepeatableWhenTwoIdenticalWorkloadsAreUsedWithIdenticalGeneratorFactories() throws Exception
    {
        for ( DriverConfiguration configuration : configurations() )
        {
            WorkloadFactory workloadFactory = new ClassNameWorkloadFactory( configuration.workloadClassName() );
            GeneratorFactory gf1 = new GeneratorFactory( new RandomDataGeneratorFactory( 42L ) );
            GeneratorFactory gf2 = new GeneratorFactory( new RandomDataGeneratorFactory( 42L ) );
            try ( Workload workloadA = workloadFactory.createWorkload();
                  Workload workloadB = workloadFactory.createWorkload() )
            {
                workloadA.init( configuration );
                workloadB.init( configuration );

                List<Class> operationsA = ImmutableList.copyOf(
                        Iterators.transform(
                                gf1.limit(
                                        WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(
                                                gf1,
                                                workloadA.streams( gf1, true )
                                        ),
                                        configuration.operationCount()
                                ),
                                new Function<Operation,Class>()
                                {
                                    @Override
                                    public Class apply( Operation operation )
                                    {
                                        return operation.getClass();
                                    }
                                }
                        )
                );

                List<Class> operationsB = ImmutableList.copyOf(
                        Iterators.transform(
                                gf1.limit(
                                        WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(
                                                gf2,
                                                workloadB.streams( gf2, true )
                                        ),
                                        configuration.operationCount()
                                ),
                                new Function<Operation,Class>()
                                {
                                    @Override
                                    public Class apply( Operation operation )
                                    {
                                        return operation.getClass();
                                    }
                                }
                        )
                );

                assertThat( operationsA.size(), is( operationsB.size() ) );

                Iterator<Class> operationsAIt = operationsA.iterator();
                Iterator<Class> operationsBIt = operationsB.iterator();

                while ( operationsAIt.hasNext() )
                {
                    Class a = operationsAIt.next();
                    Class b = operationsBIt.next();
                    assertThat( a, equalTo( b ) );
                }
            }
        }
    }

    @Ignore
    @Test
    public void shouldGenerateConfiguredQueryMix()
            throws Exception
    {
        for ( Tuple2<DriverConfiguration,Histogram<Class,Double>> configurationWithExpectedQueryMix :
                configurationsWithExpectedQueryMix() )
        {
            DriverConfiguration configuration = configurationWithExpectedQueryMix._1();
            Histogram<Class,Double> expectedQueryMix = configurationWithExpectedQueryMix._2();
            Histogram<Class,Long> actualQueryMix = new Histogram<>( 0l );
            for ( Map.Entry<Bucket<Class>,Double> bucketEntry : expectedQueryMix.getAllBuckets() )
            {
                actualQueryMix.addBucket( bucketEntry.getKey(), 0l );
            }
            WorkloadFactory workloadFactory = new ClassNameWorkloadFactory( configuration.workloadClassName() );
            try ( Workload workload = workloadFactory.createWorkload() )
            {
                workload.init( configuration );

                // When

                GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42L ) );
                Iterator<Class> operationTypes = Iterators.transform(
                        gf.limit(
                                WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(
                                        gf,
                                        workload.streams( gf, true )
                                ),
                                configuration.operationCount()
                        ),
                        new Function<Operation,Class>()
                        {
                            @Override
                            public Class apply( Operation operation )
                            {
                                return operation.getClass();
                            }
                        }
                );

                // Then

                actualQueryMix.importValueSequence( operationTypes );

                double tolerance = 0.01d;

                assertTrue(
                        format( "Distributions should be within tolerance: %s\n%s\n%s",
                                tolerance,
                                actualQueryMix.toPercentageValues().toPrettyString(),
                                expectedQueryMix.toPercentageValues().toPrettyString()
                        ),
                        Histogram.equalsWithinTolerance(
                                actualQueryMix.toPercentageValues(),
                                expectedQueryMix.toPercentageValues(),
                                tolerance
                        )
                );
            }
        }
    }

    @Test
    public void shouldLoadFromConfigFile() throws Exception
    {
        for ( DriverConfiguration configuration : configurations() )
        {
            File configurationFile = temporaryFolder.newFile();
            Files.write( configurationFile.toPath(), configuration.toPropertiesString().getBytes() );
            File resultDir = temporaryFolder.newFolder();
            assertFalse( resultDir.listFiles().length > 0 );
            assertTrue( configurationFile.exists() );

            ConsoleAndFileDriverConfiguration loadedConfiguration =
                    ConsoleAndFileDriverConfiguration.fromArgs( new String[]{
                                    "-" + ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_ARG,
                                    resultDir.getAbsolutePath(),
                                    "-P", configurationFile.getAbsolutePath()
                            }
                    );

            // When
            Client client = new Client();
            ControlService controlService = new LocalControlService(
                    timeSource.nowAsMilli(),
                    loadedConfiguration,
                    new Log4jLoggingServiceFactory( false ),
                    timeSource
            );
            ClientMode clientMode = client.getClientModeFor( controlService );
            clientMode.init();
            clientMode.startExecutionAndAwaitCompletion();

            // Then
            assertTrue( resultDir.listFiles().length > 0 );
        }
    }

    @Test
    public void shouldAssignMonotonicallyIncreasingScheduledStartTimesToOperations() throws Exception
    {
        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42L ) );

        for ( DriverConfiguration configuration : configurations() )
        {
            try ( Workload workload =
                          new ClassNameWorkloadFactory( configuration.workloadClassName() ).createWorkload() )
            {
                workload.init( configuration );

                List<Operation> operations = Lists.newArrayList(
                        gf.limit(
                                WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(
                                        gf,
                                        workload.streams( gf, true )
                                ),
                                configuration.operationCount()
                        )
                );

                long prevOperationScheduledStartTime = operations.get( 0 ).scheduledStartTimeAsMilli() - 1;
                for ( Operation operation : operations )
                {
                    assertTrue( operation.scheduledStartTimeAsMilli() >= prevOperationScheduledStartTime );
                    prevOperationScheduledStartTime = operation.scheduledStartTimeAsMilli();
                }
            }
        }
    }

    @Ignore
    @Test
    public void shouldRunWorkload() throws Exception
    {
        for ( DriverConfiguration configuration : configurations() )
        {
            File resultsDir = new File( configuration.resultDirPath() );
            assertFalse( resultsDir.listFiles().length > 0 );

            Client client = new Client();
            ControlService controlService = new LocalControlService(
                    timeSource.nowAsMilli(),
                    configuration,
                    new Log4jLoggingServiceFactory( false ),
                    timeSource
            );
            ClientMode clientMode = client.getClientModeFor( controlService );
            clientMode.init();
            clientMode.startExecutionAndAwaitCompletion();

            int expectedFileCount = (0 == configuration.warmupCount()) ? 3 : 6;
            assertThat( resultsDir.listFiles().length, is( expectedFileCount ) );

            File resultsLog = new File(
                    resultsDir,
                    configuration.name() + ThreadedQueuedMetricsService.RESULTS_LOG_FILENAME_SUFFIX
            );
            SimpleCsvFileReader csvResultsLogReader = new SimpleCsvFileReader(
                    resultsLog,
                    SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING
            );
            assertThat( (long) Iterators.size( csvResultsLogReader ), is( configuration.operationCount() ) );
        }
    }

    @Ignore
    @Test
    public void shouldCreateValidationParametersThenUseThemToPerformDatabaseValidationThenPass() throws Exception
    {
        for ( DriverConfiguration configuration : configurations() )
        {
            // **************************************************
            // where validation parameters should be written (ensure file does not yet exist)
            // **************************************************
            File validationParamsFile = temporaryFolder.newFile();
            assertThat( validationParamsFile.length(), is( 0l ) );

            ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams =
                    new ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions(
                            validationParamsFile.getAbsolutePath(),
                            500
                    );

            configuration = configuration.applyArg(
                    ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_ARG,
                    validationParams.toCommandlineString()
            );

            // **************************************************
            // create validation parameters file
            // **************************************************
            Client clientForValidationFileCreation = new Client();
            ControlService controlService = new LocalControlService(
                    timeSource.nowAsMilli(),
                    configuration,
                    new Log4jLoggingServiceFactory( false ),
                    timeSource
            );
            ClientMode clientModeForValidationFileCreation =
                    clientForValidationFileCreation.getClientModeFor( controlService );
            clientModeForValidationFileCreation.init();
            clientModeForValidationFileCreation.startExecutionAndAwaitCompletion();

            // **************************************************
            // check that validation file creation worked
            // **************************************************
            assertTrue( validationParamsFile.length() > 0 );

            // **************************************************
            // configuration for using validation parameters file to validate the database
            // **************************************************
            configuration = configuration
                    .applyArg( ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_ARG, null )
                    .applyArg(
                            ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_ARG,
                            validationParamsFile.getAbsolutePath()
                    );

            // **************************************************
            // validate the database
            // **************************************************
            Client clientForDatabaseValidation = new Client();
            controlService = new LocalControlService(
                    timeSource.nowAsMilli(),
                    configuration,
                    new Log4jLoggingServiceFactory( false ),
                    timeSource
            );
            ValidateDatabaseMode clientModeForDatabaseValidation =
                    (ValidateDatabaseMode) clientForDatabaseValidation.getClientModeFor( controlService );
            clientModeForDatabaseValidation.init();
            DbValidationResult dbValidationResult =
                    clientModeForDatabaseValidation.startExecutionAndAwaitCompletion();

            // **************************************************
            // check that validation was successful
            // **************************************************
            assertTrue( validationParamsFile.length() > 0 );
            assertThat( dbValidationResult, is( notNullValue() ) );
            assertTrue( format( "Validation with following error\n%s", dbValidationResult.resultMessage() ),
                    dbValidationResult.isSuccessful() );
        }
    }
}
