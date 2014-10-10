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
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.workloads.dummy.*;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.ldbc.driver.validation.WorkloadValidationResult.ResultType;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WorkloadValidatorTest {
    GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));

    @Test
    public void shouldTestThatAllOperationsHaveDependencyTimeSet() throws DriverConfigurationException, WorkloadException {
        // Given
        Duration maxExpectedInterleave = Duration.fromMilli(1000);

        Set<Class<? extends Operation<?>>> dependentOperationTypes = Sets.<Class<? extends Operation<?>>>newHashSet(
                TimedNamedOperation2.class
        );

        WorkloadStreams workloadStreams1 = new WorkloadStreams();
        workloadStreams1.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );

        WorkloadStreams workloadStreams2 = new WorkloadStreams();
        workloadStreams2.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );

        WorkloadStreams workloadStreams3 = new WorkloadStreams();
        workloadStreams3.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );

        WorkloadStreams workloadStreams4 = new WorkloadStreams();
        workloadStreams4.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );

        List<WorkloadStreams> validStreams = Lists.newArrayList(
                workloadStreams1,
                workloadStreams2,
                workloadStreams3,
                workloadStreams4
        );

        WorkloadStreams invalidWorkloadStreams1 = new WorkloadStreams();
        invalidWorkloadStreams1.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), null, "name1")
                ).iterator()
        );

        List<WorkloadStreams> invalidStreams = Lists.newArrayList(
                invalidWorkloadStreams1
        );

        long operationCount = validStreams.size();

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);

        WorkloadFactory validWorkloadFactory = new DummyWorkloadFactory(validStreams.iterator(), maxExpectedInterleave);
        WorkloadFactory invalidWorkloadFactory = new DummyWorkloadFactory(invalidStreams.iterator(), maxExpectedInterleave);

        // When
        WorkloadValidator validWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult validResult = validWorkloadValidator.validate(validWorkloadFactory, configuration);

        WorkloadValidator invalidWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult invalidResult = invalidWorkloadValidator.validate(invalidWorkloadFactory, configuration);

        // Then
        assertThat(validResult.errorMessage(), validResult.resultType(), is(ResultType.SUCCESSFUL));
        assertThat(validResult.errorMessage(), validResult.isSuccessful(), is(true));

        assertThat(invalidResult.errorMessage(), invalidResult.resultType(), is(ResultType.UNASSIGNED_DEPENDENCY_TIME));
        assertThat(invalidResult.errorMessage(), invalidResult.isSuccessful(), is(false));
    }

    @Test
    public void shouldTestThatDependencyTimesAreNeverGreaterThanScheduledStartTime() throws DriverConfigurationException, WorkloadException {
        // Given
        Duration maxExpectedInterleave = Duration.fromMilli(1000);

        Set<Class<? extends Operation<?>>> dependentOperationTypes = Sets.<Class<? extends Operation<?>>>newHashSet(
                TimedNamedOperation2.class
        );

        WorkloadStreams workloadStreams1 = new WorkloadStreams();
        workloadStreams1.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(11), "name1")
                ).iterator()
        );

        WorkloadStreams workloadStreams2 = new WorkloadStreams();
        workloadStreams2.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(11), "name1")
                ).iterator()
        );

        WorkloadStreams workloadStreams3 = new WorkloadStreams();
        workloadStreams3.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(11), "name1")
                ).iterator()
        );

        WorkloadStreams workloadStreams4 = new WorkloadStreams();
        workloadStreams4.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(11), "name1")
                ).iterator()
        );

        List<WorkloadStreams> validStreams = Lists.newArrayList(
                workloadStreams1,
                workloadStreams2,
                workloadStreams3,
                workloadStreams4
        );

        WorkloadStreams invalidWorkloadStreams1 = new WorkloadStreams();
        invalidWorkloadStreams1.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(13), "name1")
                ).iterator()
        );

        List<WorkloadStreams> invalidStreams = Lists.newArrayList(
                invalidWorkloadStreams1
        );

        long operationCount = validStreams.size();

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);

        WorkloadFactory validWorkloadFactory = new DummyWorkloadFactory(validStreams.iterator(), maxExpectedInterleave);
        WorkloadFactory invalidWorkloadFactory = new DummyWorkloadFactory(invalidStreams.iterator(), maxExpectedInterleave);

        // When
        WorkloadValidator validWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult validResult = validWorkloadValidator.validate(validWorkloadFactory, configuration);

        WorkloadValidator invalidWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult invalidResult = invalidWorkloadValidator.validate(invalidWorkloadFactory, configuration);

        // Then
        assertThat(validResult.errorMessage(), validResult.resultType(), is(ResultType.SUCCESSFUL));
        assertThat(validResult.errorMessage(), validResult.isSuccessful(), is(true));

        assertThat(invalidResult.errorMessage(), invalidResult.resultType(), is(ResultType.DEPENDENCY_TIME_IS_NOT_BEFORE_SCHEDULED_START_TIME));
        assertThat(invalidResult.errorMessage(), invalidResult.isSuccessful(), is(false));
    }

    @Test
    public void shouldTestThatWorkloadOnlyReturnsClassificationsForOperationsThatAreGenerated() throws DriverConfigurationException, WorkloadException {
        // Given
        Duration maxExpectedInterleave = Duration.fromMilli(1000);

        Set<Class<? extends Operation<?>>> dependentOperationTypes = Sets.<Class<? extends Operation<?>>>newHashSet(
                TimedNamedOperation2.class
        );

        WorkloadStreams workloadStreams1 = new WorkloadStreams();
        workloadStreams1.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );

        WorkloadStreams workloadStreams2 = new WorkloadStreams();
        workloadStreams2.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );

        WorkloadStreams workloadStreams3 = new WorkloadStreams();
        workloadStreams3.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );

        WorkloadStreams workloadStreams4 = new WorkloadStreams();
        workloadStreams4.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );

        List<WorkloadStreams> validStreams = Lists.newArrayList(
                workloadStreams1,
                workloadStreams2,
                workloadStreams3,
                workloadStreams4
        );

        WorkloadStreams invalidWorkloadStreams1 = new WorkloadStreams();
        invalidWorkloadStreams1.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(12), Time.fromMilli(2), "name2")
                ).iterator()
        );

        WorkloadStreams invalidWorkloadStreams2 = new WorkloadStreams();
        invalidWorkloadStreams2.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(12), Time.fromMilli(2), "name2")
                ).iterator()
        );

        List<WorkloadStreams> invalidStreams = Lists.newArrayList(
                invalidWorkloadStreams1
        );

        long operationCount = invalidStreams.size();

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);

        WorkloadFactory validWorkloadFactory = new DummyWorkloadFactory(validStreams.iterator(), maxExpectedInterleave);
        WorkloadFactory invalidWorkloadFactory = new DummyWorkloadFactory(invalidStreams.iterator(), maxExpectedInterleave);

        // When
        WorkloadValidator validWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult validResult = validWorkloadValidator.validate(validWorkloadFactory, configuration);

        WorkloadValidator invalidWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult invalidResult = invalidWorkloadValidator.validate(invalidWorkloadFactory, configuration);

        // Then
        assertThat(validResult.errorMessage(), validResult.resultType(), is(ResultType.SUCCESSFUL));
        assertThat(validResult.errorMessage(), validResult.isSuccessful(), is(true));

        assertThat(invalidResult.errorMessage(), invalidResult.resultType(), is(ResultType.OPERATION_TYPES_HAVE_CLASSIFICATIONS_BUT_WERE_NOT_GENERATED));
        assertThat(invalidResult.errorMessage(), invalidResult.isSuccessful(), is(false));
    }

    @Test
    public void shouldTestForDeterminism() throws MetricsCollectionException, DriverConfigurationException, WorkloadException {
        // Given
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Duration windowDuration = Duration.fromMilli(10);

        Set<Class<? extends Operation<?>>> dependentOperationTypes = Sets.<Class<? extends Operation<?>>>newHashSet(
                TimedNamedOperation2.class
        );

        WorkloadStreams stream1 = new WorkloadStreams();
        stream1.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );

        WorkloadStreams stream2 = new WorkloadStreams();
        stream2.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );

        WorkloadStreams stream3 = new WorkloadStreams();
        stream3.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );

        WorkloadStreams stream4 = new WorkloadStreams();
        stream4.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );

        List<WorkloadStreams> streams1 = Lists.newArrayList(
                stream1,
                stream2,
                stream3,
                stream4
        );

        WorkloadStreams stream1a = new WorkloadStreams();
        stream1a.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );

        WorkloadStreams stream2a = new WorkloadStreams();
        stream2a.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );
        WorkloadStreams stream3a = new WorkloadStreams();
        stream3a.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );
        WorkloadStreams stream4a = new WorkloadStreams();
        stream4a.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );

        List<WorkloadStreams> streams1a = Lists.newArrayList(
                stream1a,
                stream2a,
                stream3a,
                stream4a
        );

        List<Operation<?>> validAlternativeOperations = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
        );

        List<Operation<?>> invalidAlternativeOperations = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(3), "name4"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(3), "name5"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(4), "name6"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(5), "name7")
        );

        long operationCount = streams1.size();

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);

        WorkloadFactory validWorkloadFactory = new DummyWorkloadFactory(streams1.iterator(), validAlternativeOperations.iterator(), maxExpectedInterleave);
        WorkloadFactory invalidWorkloadFactory = new DummyWorkloadFactory(streams1a.iterator(), invalidAlternativeOperations.iterator(), maxExpectedInterleave);

        // When
        WorkloadValidator validWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult validResult = validWorkloadValidator.validate(validWorkloadFactory, configuration);

        WorkloadValidator invalidWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult invalidResult = invalidWorkloadValidator.validate(invalidWorkloadFactory, configuration);

        // Then
        assertThat(validResult.errorMessage(), validResult.resultType(), is(ResultType.SUCCESSFUL));
        assertThat(validResult.errorMessage(), validResult.isSuccessful(), is(true));

        assertThat(invalidResult.errorMessage(), invalidResult.resultType(), is(ResultType.WORKLOAD_IS_NOT_DETERMINISTIC));
        assertThat(invalidResult.errorMessage(), invalidResult.isSuccessful(), is(false));
    }

    @Test
    public void shouldPassWhenAllOperationsHaveStartTimesAndMaxInterleaveIsNotExceeded()
            throws DriverConfigurationException, WorkloadException {
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Time startTime = Time.fromMilli(0);
        long operationCount = 1000;

        Set<Class<? extends Operation<?>>> dependentOperationTypes = new HashSet<>();

        WorkloadStreams stream1 = new WorkloadStreams();
        stream1.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                gf.constant("name")
                        ),
                        operationCount
                )
        );

        WorkloadStreams stream2 = new WorkloadStreams();
        stream2.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                gf.constant("name")
                        ),
                        operationCount
                )
        );

        WorkloadStreams stream3 = new WorkloadStreams();
        stream3.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                gf.constant("name")
                        ),
                        operationCount
                )
        );

        WorkloadStreams stream4 = new WorkloadStreams();
        stream4.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                gf.constant("name")
                        ),
                        operationCount
                )
        );

        WorkloadStreams stream5 = new WorkloadStreams();
        stream4.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                gf.constant("name")
                        ),
                        operationCount
                )
        );

        WorkloadStreams stream6 = new WorkloadStreams();
        stream4.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                gf.constant("name")
                        ),
                        operationCount
                )
        );

        List<WorkloadStreams> streams = Lists.newArrayList(
                stream1,
                stream2,
                stream3,
                stream4,
                stream5,
                stream6
        );

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        WorkloadFactory workloadFactory = new DummyWorkloadFactory(streams.iterator(), maxExpectedInterleave);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workloadFactory, configuration);

        assertThat(result.errorMessage(), result.resultType(), is(ResultType.SUCCESSFUL));
        assertThat(result.errorMessage(), result.isSuccessful(), is(true));
    }

    @Test
    public void shouldFailWhenAllOperationsHaveStartTimesAndAllOperationsHaveClassificationsButMaxInterleaveIsExceeded()
            throws DriverConfigurationException, WorkloadException {
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Duration excessiveInterleave = maxExpectedInterleave.plus(Duration.fromNano(1));
        Time startTime = Time.fromMilli(0);
        int operationCount = 1000;

        Time lastStartTime = Lists.newArrayList(
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                gf.constant("name")
                        ),
                        operationCount - 1
                )
        ).get(operationCount - 2).scheduledStartTime();

        Set<Class<? extends Operation<?>>> dependentOperationTypes = new HashSet<>();

        WorkloadStreams stream1 = new WorkloadStreams();
        stream1.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Iterators.concat(
                        gf.limit(
                                new TimedNamedOperation1Factory(
                                        gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                        gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                        gf.constant("name")
                                ),
                                operationCount - 1
                        ),
                        gf.<Operation<?>>identity(new TimedNamedOperation1(lastStartTime.plus(excessiveInterleave), Time.fromMilli(0), "name"))
                )
        );

        WorkloadStreams stream2 = new WorkloadStreams();
        stream2.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Iterators.concat(
                        gf.limit(
                                new TimedNamedOperation1Factory(
                                        gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                        gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                        gf.constant("name")
                                ),
                                operationCount - 1
                        ),
                        gf.<Operation<?>>identity(new TimedNamedOperation1(lastStartTime.plus(excessiveInterleave), Time.fromMilli(0), "name"))
                )
        );

        List<WorkloadStreams> streams = Lists.newArrayList(
                stream1,
                stream2
        );

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        WorkloadFactory workloadFactory = new DummyWorkloadFactory(streams.iterator(), maxExpectedInterleave);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workloadFactory, configuration);

        assertThat(result.errorMessage(), result.resultType(), is(ResultType.SCHEDULED_START_TIME_INTERVAL_EXCEEDS_MAXIMUM));
        assertThat(result.errorMessage(), result.isSuccessful(), is(false));
    }

    @Test
    public void shouldFailWhenMaxInterleaveIsNotExceededButSomeOperationsHaveNoStartTime()
            throws DriverConfigurationException, WorkloadException {
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Time startTime = Time.fromMilli(0);
        long operationCount = 1000;

        Set<Class<? extends Operation<?>>> dependentOperationTypes = new HashSet<>();

        WorkloadStreams stream1 = new WorkloadStreams();
        stream1.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Iterators.concat(
                        gf.limit(
                                new TimedNamedOperation1Factory(
                                        gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                        gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                        gf.constant("name")),
                                operationCount - 1
                        ),
                        gf.identity(new NothingOperation())
                )
        );

        List<WorkloadStreams> streams = Lists.newArrayList(
                stream1
        );

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        WorkloadFactory workloadFactory = new DummyWorkloadFactory(streams.iterator(), maxExpectedInterleave);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workloadFactory, configuration);

        assertThat(result.errorMessage(), result.resultType(), is(ResultType.UNASSIGNED_SCHEDULED_START_TIME));
        assertThat(result.errorMessage(), result.isSuccessful(), is(false));
    }

    @Test
    public void shouldFailWhenOperationStartTimesAreNotMonotonicallyIncreasing()
            throws DriverConfigurationException, WorkloadException {
        Time startTime = Time.fromMilli(0);
        int operationCount = 1000;

        Time slightlyBeforeLastOperationStartTime = Lists.newArrayList(
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                gf.constant("name")),
                        operationCount - 1
                )
        ).get(operationCount - 2).scheduledStartTime().minus(Duration.fromNano(1));


        Set<Class<? extends Operation<?>>> dependentOperationTypes = new HashSet<>();

        WorkloadStreams stream1 = new WorkloadStreams();
        stream1.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Iterators.concat(
                        gf.limit(
                                new TimedNamedOperation1Factory(
                                        gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                        gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                        gf.constant("name")),
                                operationCount - 1
                        ),
                        gf.identity(new TimedNamedOperation1(slightlyBeforeLastOperationStartTime, Time.fromMilli(0), "name"))
                )
        );

        WorkloadStreams stream2 = new WorkloadStreams();
        stream2.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Iterators.concat(
                        gf.limit(
                                new TimedNamedOperation1Factory(
                                        gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                        gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                        gf.constant("name")),
                                operationCount - 1
                        ),
                        gf.identity(new TimedNamedOperation1(slightlyBeforeLastOperationStartTime, Time.fromMilli(0), "name"))
                )
        );

        List<WorkloadStreams> streams = Lists.newArrayList(
                stream1,
                stream2
        );

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        WorkloadFactory workloadFactory = new DummyWorkloadFactory(streams.iterator(), Workload.DEFAULT_MAXIMUM_EXPECTED_INTERLEAVE);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workloadFactory, configuration);

        assertThat(result.errorMessage(), result.resultType(), is(ResultType.SCHEDULED_START_TIMES_DO_NOT_INCREASE_MONOTONICALLY));
        assertThat(result.errorMessage(), result.isSuccessful(), is(false));
    }
}