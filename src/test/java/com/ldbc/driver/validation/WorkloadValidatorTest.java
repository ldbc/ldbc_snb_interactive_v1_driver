package com.ldbc.driver.validation;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.Log4jLoggingServiceFactory;
import com.ldbc.driver.control.LoggingServiceFactory;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.workloads.WorkloadFactory;
import com.ldbc.driver.workloads.dummy.DummyWorkloadFactory;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1Factory;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation2;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.ldbc.driver.validation.WorkloadValidationResult.ResultType;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WorkloadValidatorTest
{
    LoggingServiceFactory loggingServiceFactory = new Log4jLoggingServiceFactory( false );
    GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) );

    private WorkloadStreams valid1( Set<Class<? extends Operation>> dependentOperationTypes )
    {
        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                Lists.<Operation>newArrayList(
                        new TimedNamedOperation2( 10, 10, 0, "name2" ),
                        new TimedNamedOperation2( 11, 11, 1, "name2" ),
                        new TimedNamedOperation1( 12, 12, 2, "name1" )
                ).iterator(),
                null
        );
        return workloadStreams;
    }

    private WorkloadStreams invalid1( Set<Class<? extends Operation>> dependentOperationTypes )
    {
        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                Lists.<Operation>newArrayList(
                        new TimedNamedOperation2( 10, 10, 0, "name2" ),
                        new TimedNamedOperation2( 11, 11, 1, "name2" ),
                        new TimedNamedOperation1( 12, 12, -1, "name1" )
                ).iterator(),
                null
        );
        return workloadStreams;
    }

    @Test
    public void shouldTestThatAllOperationsHaveDependencyTimeSet()
            throws DriverConfigurationException, WorkloadException
    {
        // Given
        long maxExpectedInterleaveAsMilli = 1000;

        Set<Class<? extends Operation>> dependentOperationTypes = Sets.<Class<? extends Operation>>newHashSet(
                TimedNamedOperation2.class
        );

        List<WorkloadStreams> validStreams = Lists.newArrayList(
                valid1( dependentOperationTypes ),
                valid1( dependentOperationTypes ),
                valid1( dependentOperationTypes ),
                valid1( dependentOperationTypes ),
                valid1( dependentOperationTypes ),
                valid1( dependentOperationTypes ),
                valid1( dependentOperationTypes ),
                valid1( dependentOperationTypes )
        );

        List<WorkloadStreams> invalidStreams = Lists.newArrayList(
                invalid1( dependentOperationTypes ),
                invalid1( dependentOperationTypes ),
                invalid1( dependentOperationTypes ),
                invalid1( dependentOperationTypes )
        );

        long operationCount = validStreams.size();

        DriverConfiguration configuration =
                ConsoleAndFileDriverConfiguration.fromDefaults( null, null, operationCount );

        WorkloadFactory validWorkloadFactory =
                new DummyWorkloadFactory( validStreams.iterator(), maxExpectedInterleaveAsMilli );
        WorkloadFactory invalidWorkloadFactory =
                new DummyWorkloadFactory( invalidStreams.iterator(), maxExpectedInterleaveAsMilli );

        // When
        WorkloadValidator validWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult validResult = validWorkloadValidator.validate(
                validWorkloadFactory,
                configuration,
                loggingServiceFactory
        );

        WorkloadValidator invalidWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult invalidResult = invalidWorkloadValidator.validate(
                invalidWorkloadFactory,
                configuration,
                loggingServiceFactory
        );

        // Then
        assertThat( validResult.errorMessage(), validResult.resultType(), is( ResultType.SUCCESSFUL ) );
        assertThat( validResult.errorMessage(), validResult.isSuccessful(), is( true ) );

        assertThat( invalidResult.errorMessage(), invalidResult.resultType(),
                anyOf( is( ResultType.UNASSIGNED_DEPENDENCY_TIME_STAMP ), is( ResultType.UNEXPECTED ) ) );
        assertThat( invalidResult.errorMessage(), invalidResult.isSuccessful(), is( false ) );
    }

    private WorkloadStreams valid2( Set<Class<? extends Operation>> dependentOperationTypes )
    {
        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                Lists.<Operation>newArrayList(
                        new TimedNamedOperation2( 10, 10, 0, "name2" ),
                        new TimedNamedOperation2( 11, 11, 1, "name2" ),
                        new TimedNamedOperation1( 12, 12, 11, "name1" )
                ).iterator(),
                null
        );
        return workloadStreams;
    }

    private WorkloadStreams invalid2( Set<Class<? extends Operation>> dependentOperationTypes )
    {
        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                Lists.<Operation>newArrayList(
                        new TimedNamedOperation2( 10, 10, 0, "name2" ),
                        new TimedNamedOperation2( 11, 11, 1, "name2" ),
                        new TimedNamedOperation1( 12, 12, 13, "name1" )
                ).iterator(),
                null
        );
        return workloadStreams;
    }

    @Test
    public void shouldTestThatDependencyTimesAreNeverGreaterThanScheduledStartTime()
            throws DriverConfigurationException, WorkloadException
    {
        // Given
        long maxExpectedInterleaveAsMilli = 1000;

        Set<Class<? extends Operation>> dependentOperationTypes = Sets.<Class<? extends Operation>>newHashSet(
                TimedNamedOperation2.class
        );

        List<WorkloadStreams> validStreams = Lists.newArrayList(
                valid2( dependentOperationTypes ),
                valid2( dependentOperationTypes ),
                valid2( dependentOperationTypes ),
                valid2( dependentOperationTypes ),
                valid2( dependentOperationTypes ),
                valid2( dependentOperationTypes ),
                valid2( dependentOperationTypes ),
                valid2( dependentOperationTypes )
        );

        List<WorkloadStreams> invalidStreams = Lists.newArrayList(
                invalid2( dependentOperationTypes ),
                invalid2( dependentOperationTypes )
        );

        long operationCount = validStreams.size();

        DriverConfiguration configuration =
                ConsoleAndFileDriverConfiguration.fromDefaults( null, null, operationCount );

        WorkloadFactory validWorkloadFactory =
                new DummyWorkloadFactory( validStreams.iterator(), maxExpectedInterleaveAsMilli );
        WorkloadFactory invalidWorkloadFactory =
                new DummyWorkloadFactory( invalidStreams.iterator(), maxExpectedInterleaveAsMilli );

        // When
        WorkloadValidator validWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult validResult = validWorkloadValidator.validate(
                validWorkloadFactory,
                configuration,
                loggingServiceFactory
        );

        WorkloadValidator invalidWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult invalidResult =
                invalidWorkloadValidator.validate(
                        invalidWorkloadFactory,
                        configuration,
                        loggingServiceFactory
                );

        // Then
        assertThat( validResult.errorMessage(), validResult.resultType(), is( ResultType.SUCCESSFUL ) );
        assertThat( validResult.errorMessage(), validResult.isSuccessful(), is( true ) );

        assertThat( invalidResult.errorMessage(), invalidResult.resultType(),
                is( ResultType.DEPENDENCY_TIME_STAMP_IS_NOT_BEFORE_TIME_STAMP ) );
        assertThat( invalidResult.errorMessage(), invalidResult.isSuccessful(), is( false ) );
    }

    private WorkloadStreams valid3( Set<Class<? extends Operation>> dependentOperationTypes )
    {
        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                Lists.<Operation>newArrayList(
                        new TimedNamedOperation2( 10, 10, 0, "name2" ),
                        new TimedNamedOperation2( 11, 11, 1, "name2" ),
                        new TimedNamedOperation1( 12, 12, 2, "name1" )
                ).iterator(),
                null
        );
        return workloadStreams;
    }

    private WorkloadStreams invalid3( Set<Class<? extends Operation>> dependentOperationTypes )
    {
        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                Lists.<Operation>newArrayList(
                        new TimedNamedOperation2( 10, 10, 0, "name2" ),
                        new TimedNamedOperation2( 11, 11, 1, "name2" ),
                        new TimedNamedOperation2( 12, 12, 2, "name2" )
                ).iterator(),
                null
        );
        return workloadStreams;
    }

    private WorkloadStreams valid4a( Set<Class<? extends Operation>> dependentOperationTypes )
    {
        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                Lists.<Operation>newArrayList(
                        new TimedNamedOperation2( 10, 10, 0, "name2" ),
                        new TimedNamedOperation2( 11, 11, 1, "name2" ),
                        new TimedNamedOperation1( 12, 12, 2, "name1" )
                ).iterator(),
                null
        );
        return workloadStreams;
    }

    private WorkloadStreams valid4b( Set<Class<? extends Operation>> dependentOperationTypes )
    {
        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                Lists.<Operation>newArrayList(
                        new TimedNamedOperation2( 10, 10, 0, "name2" ),
                        new TimedNamedOperation2( 11, 11, 1, "name2" ),
                        new TimedNamedOperation1( 12, 12, 2, "name1" )
                ).iterator(),
                null
        );
        return workloadStreams;
    }

    @Test
    public void shouldTestForDeterminism()
            throws MetricsCollectionException, DriverConfigurationException, WorkloadException
    {
        // Given
        long maxExpectedInterleaveAsMilli = 1000;

        Set<Class<? extends Operation>> dependentOperationTypes = Sets.<Class<? extends Operation>>newHashSet(
                TimedNamedOperation2.class
        );

        List<WorkloadStreams> streams1a = Lists.newArrayList(
                valid4a( dependentOperationTypes ),
                valid4a( dependentOperationTypes ),
                valid4a( dependentOperationTypes ),
                valid4a( dependentOperationTypes ),
                valid4a( dependentOperationTypes ),
                valid4a( dependentOperationTypes ),
                valid4a( dependentOperationTypes ),
                valid4a( dependentOperationTypes ),
                valid4a( dependentOperationTypes ),
                valid4a( dependentOperationTypes ),
                valid4a( dependentOperationTypes ),
                valid4a( dependentOperationTypes ),
                valid4a( dependentOperationTypes ),
                valid4a( dependentOperationTypes ),
                valid4a( dependentOperationTypes )
        );

        List<WorkloadStreams> streams1b = Lists.newArrayList(
                valid4b( dependentOperationTypes ),
                valid4b( dependentOperationTypes ),
                valid4b( dependentOperationTypes ),
                valid4b( dependentOperationTypes ),
                valid4b( dependentOperationTypes ),
                valid4b( dependentOperationTypes ),
                valid4b( dependentOperationTypes ),
                valid4b( dependentOperationTypes ),
                valid4b( dependentOperationTypes ),
                valid4b( dependentOperationTypes ),
                valid4b( dependentOperationTypes ),
                valid4b( dependentOperationTypes ),
                valid4b( dependentOperationTypes )
        );

        List<Operation> validAlternativeOperations = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 12, 12, 2, "name1" ),
                new TimedNamedOperation1( 12, 12, 2, "name1" ),
                new TimedNamedOperation1( 12, 12, 2, "name1" ),
                new TimedNamedOperation1( 12, 12, 2, "name1" ),
                new TimedNamedOperation1( 12, 12, 2, "name1" ),
                new TimedNamedOperation1( 12, 12, 2, "name1" ),
                new TimedNamedOperation1( 12, 12, 2, "name1" ),
                new TimedNamedOperation1( 12, 12, 2, "name1" ),
                new TimedNamedOperation1( 12, 12, 2, "name1" ),
                new TimedNamedOperation1( 12, 12, 2, "name1" )
        );

        List<Operation> invalidAlternativeOperations = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 12, 12, 3, "name4" ),
                new TimedNamedOperation1( 12, 12, 3, "name5" ),
                new TimedNamedOperation1( 12, 12, 4, "name6" ),
                new TimedNamedOperation1( 12, 12, 5, "name6" ),
                new TimedNamedOperation1( 12, 12, 6, "name7" ),
                new TimedNamedOperation1( 12, 12, 7, "name8" ),
                new TimedNamedOperation1( 12, 12, 8, "name9" ),
                new TimedNamedOperation1( 12, 12, 9, "name10" ),
                new TimedNamedOperation1( 12, 12, 10, "name11" ),
                new TimedNamedOperation1( 12, 12, 11, "name12" )
        );

        long operationCount = streams1a.size();

        DriverConfiguration configuration =
                ConsoleAndFileDriverConfiguration.fromDefaults( null, null, operationCount );

        WorkloadFactory validWorkloadFactory =
                new DummyWorkloadFactory( streams1a.iterator(), validAlternativeOperations.iterator(),
                        maxExpectedInterleaveAsMilli );
        WorkloadFactory invalidWorkloadFactory =
                new DummyWorkloadFactory( streams1b.iterator(), invalidAlternativeOperations.iterator(),
                        maxExpectedInterleaveAsMilli );

        // When
        WorkloadValidator validWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult validResult = validWorkloadValidator.validate(
                validWorkloadFactory,
                configuration,
                loggingServiceFactory
        );

        WorkloadValidator invalidWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult invalidResult =
                invalidWorkloadValidator.validate(
                        invalidWorkloadFactory,
                        configuration,
                        loggingServiceFactory
                );

        // Then
        assertThat( validResult.errorMessage(), validResult.resultType(), is( ResultType.SUCCESSFUL ) );
        assertThat( validResult.errorMessage(), validResult.isSuccessful(), is( true ) );

        assertThat( invalidResult.errorMessage(), invalidResult.resultType(),
                is( ResultType.WORKLOAD_IS_NOT_DETERMINISTIC ) );
        assertThat( invalidResult.errorMessage(), invalidResult.isSuccessful(), is( false ) );
    }

    @Test
    public void shouldPassWhenAllOperationsHaveStartTimesAndMaxInterleaveIsNotExceeded()
            throws DriverConfigurationException, WorkloadException
    {
        long maxExpectedInterleaveAsMilli = 1000;
        long startTimeAsMilli = 1;
        long operationCount = 1000;

        Set<Class<? extends Operation>> dependentOperationTypes = new HashSet<>();

        WorkloadStreams stream1 = new WorkloadStreams();
        stream1.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.incrementing( startTimeAsMilli, 10l ),
                                gf.incrementing( startTimeAsMilli - 1, 0l ),
                                gf.constant( "name" )
                        ),
                        operationCount
                ),
                null
        );

        WorkloadStreams stream2 = new WorkloadStreams();
        stream2.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.incrementing( startTimeAsMilli, 10l ),
                                gf.incrementing( startTimeAsMilli - 1, 0l ),
                                gf.constant( "name" )
                        ),
                        operationCount
                ),
                null
        );

        WorkloadStreams stream3 = new WorkloadStreams();
        stream3.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.incrementing( startTimeAsMilli, 10l ),
                                gf.incrementing( startTimeAsMilli - 1, 0l ),
                                gf.constant( "name" )
                        ),
                        operationCount
                ),
                null
        );

        WorkloadStreams stream4 = new WorkloadStreams();
        stream4.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.incrementing( startTimeAsMilli, 10l ),
                                gf.incrementing( startTimeAsMilli - 1, 0l ),
                                gf.constant( "name" )
                        ),
                        operationCount
                ),
                null
        );

        WorkloadStreams stream5 = new WorkloadStreams();
        stream5.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.incrementing( startTimeAsMilli, 10l ),
                                gf.incrementing( startTimeAsMilli - 1, 0l ),
                                gf.constant( "name" )
                        ),
                        operationCount
                ),
                null
        );

        WorkloadStreams stream6 = new WorkloadStreams();
        stream6.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.incrementing( startTimeAsMilli, 10l ),
                                gf.incrementing( startTimeAsMilli - 1, 0l ),
                                gf.constant( "name" )
                        ),
                        operationCount
                ),
                null
        );

        WorkloadStreams stream7 = new WorkloadStreams();
        stream7.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.incrementing( startTimeAsMilli, 10l ),
                                gf.incrementing( startTimeAsMilli - 1, 0l ),
                                gf.constant( "name" )
                        ),
                        operationCount
                ),
                null
        );

        WorkloadStreams stream8 = new WorkloadStreams();
        stream8.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.incrementing( startTimeAsMilli, 10l ),
                                gf.incrementing( startTimeAsMilli - 1, 0l ),
                                gf.constant( "name" )
                        ),
                        operationCount
                ),
                null
        );

        List<WorkloadStreams> streams = Lists.newArrayList(
                stream1,
                stream2,
                stream3,
                stream4,
                stream5,
                stream6,
                stream7,
                stream8
        );

        DriverConfiguration configuration =
                ConsoleAndFileDriverConfiguration.fromDefaults( null, null, operationCount );
        WorkloadFactory workloadFactory = new DummyWorkloadFactory( streams.iterator(), maxExpectedInterleaveAsMilli );
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(
                workloadFactory,
                configuration,
                loggingServiceFactory
        );

        assertThat( result.errorMessage(), result.resultType(), is( ResultType.SUCCESSFUL ) );
        assertThat( result.errorMessage(), result.isSuccessful(), is( true ) );
    }

    @Test
    public void shouldFailWhenAllOperationsHaveStartTimesButMaxInterleaveIsExceeded()
            throws DriverConfigurationException, WorkloadException
    {
        long maxExpectedInterleaveAsMilli = 1000;
        long excessiveInterleaveAsMilli = maxExpectedInterleaveAsMilli + 1;
        long startTimeAsMilli = 1;
        int operationCount = 1000;

        long lastStartTimeAsMilli = Lists.newArrayList(
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.incrementing( startTimeAsMilli, 10l ),
                                gf.incrementing( startTimeAsMilli - 1, 0l ),
                                gf.constant( "name" )
                        ),
                        operationCount - 1
                )
        ).get( operationCount - 2 ).scheduledStartTimeAsMilli();

        Set<Class<? extends Operation>> dependentOperationTypes = new HashSet<>();

        WorkloadStreams stream1 = new WorkloadStreams();
        stream1.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                Iterators.concat(
                        gf.limit(
                                new TimedNamedOperation1Factory(
                                        gf.incrementing( startTimeAsMilli, 10l ),
                                        gf.incrementing( startTimeAsMilli - 1, 0l ),
                                        gf.constant( "name" )
                                ),
                                operationCount - 1
                        ),
                        gf.<Operation>identity(
                                new TimedNamedOperation1(
                                        lastStartTimeAsMilli + excessiveInterleaveAsMilli,
                                        lastStartTimeAsMilli + excessiveInterleaveAsMilli,
                                        0,
                                        "name"
                                )
                        )
                ),
                null
        );

        WorkloadStreams stream2 = new WorkloadStreams();
        stream2.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                Iterators.concat(
                        gf.limit(
                                new TimedNamedOperation1Factory(
                                        gf.incrementing( startTimeAsMilli, 10l ),
                                        gf.incrementing( startTimeAsMilli - 1l, 0l ),
                                        gf.constant( "name" )
                                ),
                                operationCount - 1
                        ),
                        gf.<Operation>identity(
                                new TimedNamedOperation1(
                                        lastStartTimeAsMilli + excessiveInterleaveAsMilli,
                                        lastStartTimeAsMilli + excessiveInterleaveAsMilli,
                                        0,
                                        "name"
                                )
                        )
                ),
                null
        );

        WorkloadStreams stream3 = new WorkloadStreams();
        stream3.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                Iterators.concat(
                        gf.limit(
                                new TimedNamedOperation1Factory(
                                        gf.incrementing( startTimeAsMilli, 10l ),
                                        gf.incrementing( startTimeAsMilli - 1l, 0l ),
                                        gf.constant( "name" )
                                ),
                                operationCount - 1
                        ),
                        gf.<Operation>identity(
                                new TimedNamedOperation1(
                                        lastStartTimeAsMilli + excessiveInterleaveAsMilli,
                                        lastStartTimeAsMilli + excessiveInterleaveAsMilli,
                                        0,
                                        "name"
                                )
                        )
                ),
                null
        );

        WorkloadStreams stream4 = new WorkloadStreams();
        stream4.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                Iterators.concat(
                        gf.limit(
                                new TimedNamedOperation1Factory(
                                        gf.incrementing( startTimeAsMilli, 10l ),
                                        gf.incrementing( startTimeAsMilli - 1l, 0l ),
                                        gf.constant( "name" )
                                ),
                                operationCount - 1
                        ),
                        gf.<Operation>identity(
                                new TimedNamedOperation1(
                                        lastStartTimeAsMilli + excessiveInterleaveAsMilli,
                                        lastStartTimeAsMilli + excessiveInterleaveAsMilli,
                                        0,
                                        "name"
                                )
                        )
                ),
                null
        );

        List<WorkloadStreams> streams = Lists.newArrayList(
                stream1,
                stream2,
                stream3,
                stream4
        );

        DriverConfiguration configuration =
                ConsoleAndFileDriverConfiguration.fromDefaults( null, null, operationCount );
        WorkloadFactory workloadFactory = new DummyWorkloadFactory( streams.iterator(), maxExpectedInterleaveAsMilli );
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(
                workloadFactory,
                configuration,
                loggingServiceFactory
        );

        assertThat( result.errorMessage(), result.resultType(),
                is( ResultType.SCHEDULED_START_TIME_INTERVAL_EXCEEDS_MAXIMUM ) );
        assertThat( result.errorMessage(), result.isSuccessful(), is( false ) );
    }

    @Test
    public void shouldFailWhenOperationStartTimesAreNotMonotonicallyIncreasing()
            throws DriverConfigurationException, WorkloadException
    {
        long startTimeAsMilli = 1;
        int operationCount = 1000;

        long slightlyBeforeLastOperationStartTimeAsMilli = Lists.newArrayList(
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.incrementing( startTimeAsMilli, 10l ),
                                gf.incrementing( startTimeAsMilli - 1l, 0l ),
                                gf.constant( "name" ) ),
                        operationCount - 1
                )
        ).get( operationCount - 2 ).scheduledStartTimeAsMilli() - 1;


        Set<Class<? extends Operation>> dependentOperationTypes = new HashSet<>();

        WorkloadStreams stream1 = new WorkloadStreams();
        stream1.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                Iterators.concat(
                        gf.limit(
                                new TimedNamedOperation1Factory(
                                        gf.incrementing( startTimeAsMilli, 10l ),
                                        gf.incrementing( startTimeAsMilli - 1l, 0l ),
                                        gf.constant( "name" ) ),
                                operationCount - 1
                        ),
                        gf.identity(
                                new TimedNamedOperation1(
                                        slightlyBeforeLastOperationStartTimeAsMilli,
                                        slightlyBeforeLastOperationStartTimeAsMilli,
                                        0,
                                        "name"
                                )
                        )
                ),
                null
        );

        WorkloadStreams stream2 = new WorkloadStreams();
        stream2.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                Iterators.concat(
                        gf.limit(
                                new TimedNamedOperation1Factory(
                                        gf.incrementing( startTimeAsMilli, 10l ),
                                        gf.incrementing( startTimeAsMilli - 1l, 0l ),
                                        gf.constant( "name" ) ),
                                operationCount - 1
                        ),
                        gf.identity(
                                new TimedNamedOperation1(
                                        slightlyBeforeLastOperationStartTimeAsMilli,
                                        slightlyBeforeLastOperationStartTimeAsMilli,
                                        0,
                                        "name"
                                )
                        )
                ),
                null
        );

        WorkloadStreams stream3 = new WorkloadStreams();
        stream3.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                Iterators.concat(
                        gf.limit(
                                new TimedNamedOperation1Factory(
                                        gf.incrementing( startTimeAsMilli, 10l ),
                                        gf.incrementing( startTimeAsMilli - 1l, 0l ),
                                        gf.constant( "name" ) ),
                                operationCount - 1
                        ),
                        gf.identity(
                                new TimedNamedOperation1(
                                        slightlyBeforeLastOperationStartTimeAsMilli,
                                        slightlyBeforeLastOperationStartTimeAsMilli,
                                        0,
                                        "name"
                                )
                        )
                ),
                null
        );

        WorkloadStreams stream4 = new WorkloadStreams();
        stream4.setAsynchronousStream(
                dependentOperationTypes,
                new HashSet<Class<? extends Operation>>(),
                Collections.<Operation>emptyIterator(),
                Iterators.concat(
                        gf.limit(
                                new TimedNamedOperation1Factory(
                                        gf.incrementing( startTimeAsMilli, 10l ),
                                        gf.incrementing( startTimeAsMilli - 1l, 0l ),
                                        gf.constant( "name" ) ),
                                operationCount - 1
                        ),
                        gf.identity(
                                new TimedNamedOperation1(
                                        slightlyBeforeLastOperationStartTimeAsMilli,
                                        slightlyBeforeLastOperationStartTimeAsMilli,
                                        0,
                                        "name"
                                )
                        )
                ),
                null
        );

        List<WorkloadStreams> streams = Lists.newArrayList(
                stream1,
                stream2,
                stream3,
                stream4
        );

        DriverConfiguration configuration =
                ConsoleAndFileDriverConfiguration.fromDefaults( null, null, operationCount );
        WorkloadFactory workloadFactory =
                new DummyWorkloadFactory( streams.iterator(), Workload.DEFAULT_MAXIMUM_EXPECTED_INTERLEAVE_AS_MILLI );
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(
                workloadFactory,
                configuration,
                loggingServiceFactory
        );

        assertThat( result.errorMessage(), result.resultType(),
                is( ResultType.SCHEDULED_START_TIMES_DO_NOT_INCREASE_MONOTONICALLY ) );
        assertThat( result.errorMessage(), result.isSuccessful(), is( false ) );
    }
}