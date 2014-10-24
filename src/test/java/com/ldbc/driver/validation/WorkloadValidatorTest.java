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

    private WorkloadStreams valid1(Set<Class<? extends Operation<?>>> dependentOperationTypes){
        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );
        return workloadStreams;
    }

    private WorkloadStreams invalid1(Set<Class<? extends Operation<?>>> dependentOperationTypes){
        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), null, "name1")
                ).iterator()
        );
        return workloadStreams;
    }
    @Test
    public void shouldTestThatAllOperationsHaveDependencyTimeSet() throws DriverConfigurationException, WorkloadException {
        // Given
        Duration maxExpectedInterleave = Duration.fromMilli(1000);

        Set<Class<? extends Operation<?>>> dependentOperationTypes = Sets.<Class<? extends Operation<?>>>newHashSet(
                TimedNamedOperation2.class
        );

        List<WorkloadStreams> validStreams = Lists.newArrayList(
                valid1(dependentOperationTypes),
                valid1(dependentOperationTypes),
                valid1(dependentOperationTypes),
                valid1(dependentOperationTypes),
                valid1(dependentOperationTypes),
                valid1(dependentOperationTypes),
                valid1(dependentOperationTypes),
                valid1(dependentOperationTypes)
        );

        List<WorkloadStreams> invalidStreams = Lists.newArrayList(
                invalid1(dependentOperationTypes),
                invalid1(dependentOperationTypes),
                invalid1(dependentOperationTypes),
                invalid1(dependentOperationTypes)
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

    private WorkloadStreams valid2(Set<Class<? extends Operation<?>>> dependentOperationTypes){
        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(11), "name1")
                ).iterator()
        );
        return workloadStreams;
    }

    private WorkloadStreams invalid2(Set<Class<? extends Operation<?>>> dependentOperationTypes){
        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(13), "name1")
                ).iterator()
        );
        return workloadStreams;
    }
    @Test
    public void shouldTestThatDependencyTimesAreNeverGreaterThanScheduledStartTime() throws DriverConfigurationException, WorkloadException {
        // Given
        Duration maxExpectedInterleave = Duration.fromMilli(1000);

        Set<Class<? extends Operation<?>>> dependentOperationTypes = Sets.<Class<? extends Operation<?>>>newHashSet(
                TimedNamedOperation2.class
        );

        List<WorkloadStreams> validStreams = Lists.newArrayList(
                valid2(dependentOperationTypes),
                valid2(dependentOperationTypes),
                valid2(dependentOperationTypes),
                valid2(dependentOperationTypes),
                valid2(dependentOperationTypes),
                valid2(dependentOperationTypes),
                valid2(dependentOperationTypes),
                valid2(dependentOperationTypes)
        );

        List<WorkloadStreams> invalidStreams = Lists.newArrayList(
                invalid2(dependentOperationTypes),
                invalid2(dependentOperationTypes)
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

    private WorkloadStreams valid3(Set<Class<? extends Operation<?>>> dependentOperationTypes){
        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );
        return workloadStreams;
    }

    private WorkloadStreams invalid3(Set<Class<? extends Operation<?>>> dependentOperationTypes){
        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(12), Time.fromMilli(2), "name2")
                ).iterator()
        );
        return workloadStreams;
    }

    private WorkloadStreams valid4a(Set<Class<? extends Operation<?>>> dependentOperationTypes){
        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );
        return workloadStreams;
    }

    private WorkloadStreams valid4b(Set<Class<? extends Operation<?>>> dependentOperationTypes){
        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                dependentOperationTypes,
                Collections.<Operation<?>>emptyIterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );
        return workloadStreams;
    }
    @Test
    public void shouldTestForDeterminism() throws MetricsCollectionException, DriverConfigurationException, WorkloadException {
        // Given
        Duration maxExpectedInterleave = Duration.fromMilli(1000);

        Set<Class<? extends Operation<?>>> dependentOperationTypes = Sets.<Class<? extends Operation<?>>>newHashSet(
                TimedNamedOperation2.class
        );

        List<WorkloadStreams> streams1a = Lists.newArrayList(
                valid4a(dependentOperationTypes),
                valid4a(dependentOperationTypes),
                valid4a(dependentOperationTypes),
                valid4a(dependentOperationTypes),
                valid4a(dependentOperationTypes),
                valid4a(dependentOperationTypes),
                valid4a(dependentOperationTypes),
                valid4a(dependentOperationTypes),
                valid4a(dependentOperationTypes),
                valid4a(dependentOperationTypes),
                valid4a(dependentOperationTypes),
                valid4a(dependentOperationTypes),
                valid4a(dependentOperationTypes),
                valid4a(dependentOperationTypes),
                valid4a(dependentOperationTypes)
        );

        List<WorkloadStreams> streams1b = Lists.newArrayList(
                valid4b(dependentOperationTypes),
                valid4b(dependentOperationTypes),
                valid4b(dependentOperationTypes),
                valid4b(dependentOperationTypes),
                valid4b(dependentOperationTypes),
                valid4b(dependentOperationTypes),
                valid4b(dependentOperationTypes),
                valid4b(dependentOperationTypes),
                valid4b(dependentOperationTypes),
                valid4b(dependentOperationTypes),
                valid4b(dependentOperationTypes),
                valid4b(dependentOperationTypes),
                valid4b(dependentOperationTypes)
        );

        List<Operation<?>> validAlternativeOperations = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
        );

        List<Operation<?>> invalidAlternativeOperations = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(3), "name4"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(3), "name5"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(4), "name6"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(5), "name6"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(6), "name7"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(7), "name8"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(8), "name9"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(9), "name10"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(10), "name11"),
                new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(11), "name12")
        );

        long operationCount = streams1a.size();

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);

        WorkloadFactory validWorkloadFactory = new DummyWorkloadFactory(streams1a.iterator(), validAlternativeOperations.iterator(), maxExpectedInterleave);
        WorkloadFactory invalidWorkloadFactory = new DummyWorkloadFactory(streams1b.iterator(), invalidAlternativeOperations.iterator(), maxExpectedInterleave);

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
        stream5.setAsynchronousStream(
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
        stream6.setAsynchronousStream(
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

        WorkloadStreams stream7 = new WorkloadStreams();
        stream7.setAsynchronousStream(
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

        WorkloadStreams stream8 = new WorkloadStreams();
        stream8.setAsynchronousStream(
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
                stream6,
                stream7,
                stream8
        );

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        WorkloadFactory workloadFactory = new DummyWorkloadFactory(streams.iterator(), maxExpectedInterleave);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workloadFactory, configuration);

        assertThat(result.errorMessage(), result.resultType(), is(ResultType.SUCCESSFUL));
        assertThat(result.errorMessage(), result.isSuccessful(), is(true));
    }

    @Test
    public void shouldFailWhenAllOperationsHaveStartTimesButMaxInterleaveIsExceeded()
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
        ).get(operationCount - 2).scheduledStartTimeAsMilli();

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

        WorkloadStreams stream3 = new WorkloadStreams();
        stream3.setAsynchronousStream(
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

        WorkloadStreams stream4 = new WorkloadStreams();
        stream4.setAsynchronousStream(
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
                stream2,
                stream3,
                stream4
        );

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        WorkloadFactory workloadFactory = new DummyWorkloadFactory(streams.iterator(), maxExpectedInterleave);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workloadFactory, configuration);

        assertThat(result.errorMessage(), result.resultType(), is(ResultType.SCHEDULED_START_TIME_INTERVAL_EXCEEDS_MAXIMUM));
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
        ).get(operationCount - 2).scheduledStartTimeAsMilli().minus(Duration.fromNano(1));


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

        WorkloadStreams stream3 = new WorkloadStreams();
        stream3.setAsynchronousStream(
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

        WorkloadStreams stream4 = new WorkloadStreams();
        stream4.setAsynchronousStream(
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
                stream2,
                stream3,
                stream4
        );

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        WorkloadFactory workloadFactory = new DummyWorkloadFactory(streams.iterator(), Workload.DEFAULT_MAXIMUM_EXPECTED_INTERLEAVE);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workloadFactory, configuration);

        assertThat(result.errorMessage(), result.resultType(), is(ResultType.SCHEDULED_START_TIMES_DO_NOT_INCREASE_MONOTONICALLY));
        assertThat(result.errorMessage(), result.isSuccessful(), is(false));
    }
}