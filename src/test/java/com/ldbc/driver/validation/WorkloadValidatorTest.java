package com.ldbc.driver.validation;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationClassification;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.testutils.*;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.ldbc.driver.validation.WorkloadValidationResult.ResultType;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WorkloadValidatorTest {
    GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));

    @Test
    public void shouldTestDifferenceBetweenStartTimeAndDependencyTimeAtLeastWindowDurationForOperationsExecutedUsingWindowedSchedulingModeAndGctModeNotNone()
            throws DriverConfigurationException, WorkloadException {
        // Given
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Duration windowDuration = Duration.fromMilli(10);

        List<Operation<?>> validOperations = Lists.<Operation<?>>newArrayList(
                new TimedOperation(Time.fromMilli(10), Time.fromMilli(0)),
                new TimedOperation(Time.fromMilli(11), Time.fromMilli(1)),
                new TimedNamedOperation(Time.fromMilli(12), Time.fromMilli(3), "name")
        );

        List<Operation<?>> invalidOperations = Lists.<Operation<?>>newArrayList(
                new TimedOperation(Time.fromMilli(10), Time.fromMilli(0)),
                new TimedOperation(Time.fromMilli(11), Time.fromMilli(1)),
                new TimedOperation(Time.fromMilli(12), Time.fromMilli(3))
        );

        assertThat(validOperations.size(), is(invalidOperations.size()));

        long operationCount = validOperations.size();

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.WINDOWED, OperationClassification.GctMode.READ));
        operationClassifications.put(
                TimedNamedOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.NONE));

        Map<String, String> nonDefaultParams = new HashMap<>();
        nonDefaultParams.put(ConsoleAndFileDriverConfiguration.WINDOWED_EXECUTION_WINDOW_DURATION_ARG, Long.toString(windowDuration.asMilli()));
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount).applyMap(nonDefaultParams);

        Workload validWorkload = new DummyWorkload(validOperations.iterator(), operationClassifications, maxExpectedInterleave);
        validWorkload.init(configuration);

        Workload invalidWorkload = new DummyWorkload(invalidOperations.iterator(), operationClassifications, maxExpectedInterleave);
        invalidWorkload.init(configuration);

        // When
        WorkloadValidator validWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult validResult = validWorkloadValidator.validate(validWorkload, configuration);

        WorkloadValidator invalidWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult invalidResult = invalidWorkloadValidator.validate(invalidWorkload, configuration);

        // Then
        System.out.println(validResult.errorMessage());
        assertThat(validResult.resultType(), is(ResultType.SUCCESSFUL));
        assertThat(validResult.errorMessage(), validResult.isSuccessful(), is(true));

        System.out.println(invalidResult.errorMessage());
        assertThat(invalidResult.resultType(), is(ResultType.INSUFFICIENT_INTERVAL_BETWEEN_DEPENDENCY_TIME_AND_SCHEDULED_START_TIME));
        assertThat(invalidResult.isSuccessful(), is(false));
    }

    @Test
    public void shouldTestThatAllOperationsWithGctModeNotNoneHaveDependencyTimeSet() throws DriverConfigurationException, WorkloadException {
        // Given
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Duration windowDuration = Duration.fromMilli(10);

        List<Operation<?>> validOperations = Lists.<Operation<?>>newArrayList(
                new TimedOperation(Time.fromMilli(10), Time.fromMilli(0)),
                new TimedOperation(Time.fromMilli(11), Time.fromMilli(1)),
                new TimedNamedOperation(Time.fromMilli(12), null, "name")
        );

        List<Operation<?>> invalidOperations = Lists.<Operation<?>>newArrayList(
                new TimedOperation(Time.fromMilli(10), Time.fromMilli(0)),
                new TimedOperation(Time.fromMilli(11), Time.fromMilli(1)),
                new TimedOperation(Time.fromMilli(12), null)
        );

        assertThat(validOperations.size(), is(invalidOperations.size()));

        long operationCount = validOperations.size();

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.WINDOWED, OperationClassification.GctMode.READ));
        operationClassifications.put(
                TimedNamedOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.NONE));

        Map<String, String> nonDefaultParams = new HashMap<>();
        nonDefaultParams.put(ConsoleAndFileDriverConfiguration.WINDOWED_EXECUTION_WINDOW_DURATION_ARG, Long.toString(windowDuration.asMilli()));
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount).applyMap(nonDefaultParams);

        Workload validWorkload = new DummyWorkload(validOperations.iterator(), operationClassifications, maxExpectedInterleave);
        validWorkload.init(configuration);

        Workload invalidWorkload = new DummyWorkload(invalidOperations.iterator(), operationClassifications, maxExpectedInterleave);
        invalidWorkload.init(configuration);

        // When
        WorkloadValidator validWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult validResult = validWorkloadValidator.validate(validWorkload, configuration);

        WorkloadValidator invalidWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult invalidResult = invalidWorkloadValidator.validate(invalidWorkload, configuration);

        // Then
        System.out.println(validResult.errorMessage());
        assertThat(validResult.resultType(), is(ResultType.SUCCESSFUL));
        assertThat(validResult.errorMessage(), validResult.isSuccessful(), is(true));

        System.out.println(invalidResult.errorMessage());
        assertThat(invalidResult.resultType(), is(ResultType.UNASSIGNED_DEPENDENCY_TIME));
        assertThat(invalidResult.isSuccessful(), is(false));
    }

    @Test
    public void shouldTestThatDependencyTimesAreNeverGreaterThanScheduledStartTimeForOperationsWithGctModeNotNone() throws DriverConfigurationException, WorkloadException {
        // Given
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Duration windowDuration = Duration.fromMilli(10);

        List<Operation<?>> validOperations = Lists.<Operation<?>>newArrayList(
                new TimedOperation(Time.fromMilli(10), Time.fromMilli(0)),
                new TimedOperation(Time.fromMilli(11), Time.fromMilli(1)),
                new TimedNamedOperation(Time.fromMilli(12), Time.fromMilli(13), "name")
        );

        List<Operation<?>> invalidOperations = Lists.<Operation<?>>newArrayList(
                new TimedOperation(Time.fromMilli(10), Time.fromMilli(0)),
                new TimedOperation(Time.fromMilli(11), Time.fromMilli(1)),
                new TimedOperation(Time.fromMilli(12), Time.fromMilli(13))
        );

        assertThat(validOperations.size(), is(invalidOperations.size()));

        long operationCount = validOperations.size();

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.READ));
        operationClassifications.put(
                TimedNamedOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.NONE));

        Map<String, String> nonDefaultParams = new HashMap<>();
        nonDefaultParams.put(ConsoleAndFileDriverConfiguration.WINDOWED_EXECUTION_WINDOW_DURATION_ARG, Long.toString(windowDuration.asMilli()));
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount).applyMap(nonDefaultParams);

        Workload validWorkload = new DummyWorkload(validOperations.iterator(), operationClassifications, maxExpectedInterleave);
        validWorkload.init(configuration);

        Workload invalidWorkload = new DummyWorkload(invalidOperations.iterator(), operationClassifications, maxExpectedInterleave);
        invalidWorkload.init(configuration);

        // When
        WorkloadValidator validWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult validResult = validWorkloadValidator.validate(validWorkload, configuration);

        WorkloadValidator invalidWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult invalidResult = invalidWorkloadValidator.validate(invalidWorkload, configuration);

        // Then
        System.out.println(validResult.errorMessage());
        assertThat(validResult.resultType(), is(ResultType.SUCCESSFUL));
        assertThat(validResult.errorMessage(), validResult.isSuccessful(), is(true));

        System.out.println(invalidResult.errorMessage());
        assertThat(invalidResult.resultType(), is(ResultType.DEPENDENCY_TIME_IS_LATER_THAN_SCHEDULED_START_TIME));
        assertThat(invalidResult.isSuccessful(), is(false));
    }

    @Test
    public void shouldTestThatWorkloadOnlyReturnsClassificationsForOperationsThatAreReturned() throws DriverConfigurationException, WorkloadException {
        // Given
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Duration windowDuration = Duration.fromMilli(10);

        List<Operation<?>> validOperations = Lists.<Operation<?>>newArrayList(
                new TimedOperation(Time.fromMilli(10), Time.fromMilli(0)),
                new TimedOperation(Time.fromMilli(11), Time.fromMilli(1)),
                new TimedNamedOperation(Time.fromMilli(12), Time.fromMilli(2), "name")
        );

        List<Operation<?>> invalidOperations = Lists.<Operation<?>>newArrayList(
                new TimedOperation(Time.fromMilli(10), Time.fromMilli(0)),
                new TimedOperation(Time.fromMilli(11), Time.fromMilli(1)),
                new TimedOperation(Time.fromMilli(12), Time.fromMilli(2))
        );

        assertThat(validOperations.size(), is(invalidOperations.size()));

        long operationCount = validOperations.size();

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.READ));
        operationClassifications.put(
                TimedNamedOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.NONE));

        Map<String, String> nonDefaultParams = new HashMap<>();
        nonDefaultParams.put(ConsoleAndFileDriverConfiguration.WINDOWED_EXECUTION_WINDOW_DURATION_ARG, Long.toString(windowDuration.asMilli()));
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount).applyMap(nonDefaultParams);

        Workload validWorkload = new DummyWorkload(validOperations.iterator(), operationClassifications, maxExpectedInterleave);
        validWorkload.init(configuration);

        Workload invalidWorkload = new DummyWorkload(invalidOperations.iterator(), operationClassifications, maxExpectedInterleave);
        invalidWorkload.init(configuration);

        // When
        WorkloadValidator validWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult validResult = validWorkloadValidator.validate(validWorkload, configuration);

        WorkloadValidator invalidWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult invalidResult = invalidWorkloadValidator.validate(invalidWorkload, configuration);

        // Then
        System.out.println(validResult.errorMessage());
        assertThat(validResult.resultType(), is(ResultType.SUCCESSFUL));
        assertThat(validResult.errorMessage(), validResult.isSuccessful(), is(true));

        System.out.println(invalidResult.errorMessage());
        assertThat(invalidResult.resultType(), is(ResultType.OPERATION_TYPES_HAVE_CLASSIFICATIONS_BUT_WERE_NOT_GENERATED));
        assertThat(invalidResult.isSuccessful(), is(false));
    }

    @Test
    public void shouldTestForDeterminism() throws MetricsCollectionException, DriverConfigurationException, WorkloadException {
        // Given
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Duration windowDuration = Duration.fromMilli(10);

        List<Operation<?>> operations = Lists.<Operation<?>>newArrayList(
                new TimedOperation(Time.fromMilli(10), Time.fromMilli(0)),
                new TimedOperation(Time.fromMilli(11), Time.fromMilli(1)),
                new TimedNamedOperation(Time.fromMilli(12), Time.fromMilli(2), "name")
        );

        List<Operation<?>> validAlternativeOperations = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation(Time.fromMilli(12), Time.fromMilli(2), "name"),
                new TimedNamedOperation(Time.fromMilli(12), Time.fromMilli(2), "name"),
                new TimedNamedOperation(Time.fromMilli(12), Time.fromMilli(2), "name")
        );

        List<Operation<?>> invalidAlternativeOperations = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation(Time.fromMilli(12), Time.fromMilli(2), "name1"),
                new TimedNamedOperation(Time.fromMilli(12), Time.fromMilli(2), "name2"),
                new TimedNamedOperation(Time.fromMilli(12), Time.fromMilli(2), "name3")
        );

        long operationCount = operations.size();

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.READ));
        operationClassifications.put(
                TimedNamedOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.NONE));

        Map<String, String> nonDefaultParams = new HashMap<>();
        nonDefaultParams.put(ConsoleAndFileDriverConfiguration.WINDOWED_EXECUTION_WINDOW_DURATION_ARG, Long.toString(windowDuration.asMilli()));
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount).applyMap(nonDefaultParams);

        Workload validWorkload = new DummyWorkload(operations.iterator(), validAlternativeOperations.iterator(), operationClassifications, maxExpectedInterleave);
        validWorkload.init(configuration);

        Workload invalidWorkload = new DummyWorkload(operations.iterator(), invalidAlternativeOperations.iterator(), operationClassifications, maxExpectedInterleave);
        invalidWorkload.init(configuration);

        // When
        WorkloadValidator validWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult validResult = validWorkloadValidator.validate(validWorkload, configuration);

        WorkloadValidator invalidWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult invalidResult = invalidWorkloadValidator.validate(invalidWorkload, configuration);

        // Then
        System.out.println(validResult.errorMessage());
        assertThat(validResult.resultType(), is(ResultType.SUCCESSFUL));
        assertThat(validResult.errorMessage(), validResult.isSuccessful(), is(true));

        System.out.println(invalidResult.errorMessage());
        assertThat(invalidResult.resultType(), is(ResultType.WORKLOAD_IS_NOT_DETERMINISTIC));
        assertThat(invalidResult.isSuccessful(), is(false));
    }

    @Test
    public void shouldTestThatAllOperationClassificationsContainSchedulingMode() throws DriverConfigurationException, WorkloadException {
        // Given
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Duration windowDuration = Duration.fromMilli(10);

        List<Operation<?>> operations = Lists.<Operation<?>>newArrayList(
                new TimedOperation(Time.fromMilli(10), Time.fromMilli(0)),
                new TimedOperation(Time.fromMilli(11), Time.fromMilli(1)),
                new TimedNamedOperation(Time.fromMilli(12), Time.fromMilli(2), "name")
        );

        long operationCount = operations.size();

        Map<Class<? extends Operation>, OperationClassification> validOperationClassifications = new HashMap<>();
        validOperationClassifications.put(
                TimedOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.READ));
        validOperationClassifications.put(
                TimedNamedOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.NONE));

        Map<Class<? extends Operation>, OperationClassification> invalidOperationClassifications = new HashMap<>();
        invalidOperationClassifications.put(
                TimedOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.READ));
        invalidOperationClassifications.put(
                TimedNamedOperation.class,
                new OperationClassification(null, OperationClassification.GctMode.NONE));

        Map<String, String> nonDefaultParams = new HashMap<>();
        nonDefaultParams.put(ConsoleAndFileDriverConfiguration.WINDOWED_EXECUTION_WINDOW_DURATION_ARG, Long.toString(windowDuration.asMilli()));
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount).applyMap(nonDefaultParams);

        Workload validWorkload = new DummyWorkload(operations.iterator(), validOperationClassifications, maxExpectedInterleave);
        validWorkload.init(configuration);

        Workload invalidWorkload = new DummyWorkload(operations.iterator(), invalidOperationClassifications, maxExpectedInterleave);
        invalidWorkload.init(configuration);

        // When
        WorkloadValidator validWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult validResult = validWorkloadValidator.validate(validWorkload, configuration);

        WorkloadValidator invalidWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult invalidResult = invalidWorkloadValidator.validate(invalidWorkload, configuration);

        // Then
        System.out.println(validResult.errorMessage());
        assertThat(validResult.resultType(), is(ResultType.SUCCESSFUL));
        assertThat(validResult.errorMessage(), validResult.isSuccessful(), is(true));

        System.out.println(invalidResult.errorMessage());
        assertThat(invalidResult.resultType(), is(ResultType.OPERATION_CLASSIFICATION_HAS_NO_SCHEDULING_MODE));
        assertThat(invalidResult.isSuccessful(), is(false));
    }

    @Test
    public void shouldTestThatAllOperationClassificationsContainGctMode()
            throws DriverConfigurationException, WorkloadException {
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Time startTime = Time.fromMilli(0);
        long operationCount = 1000;
        Iterator<Time> startTimes = gf.constantIncrementTime(startTime, Duration.fromMilli(10));
        Iterator<Time> dependencyTimes = gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0));
        Iterator<Operation<?>> timedNothingOperations = new TimedNothingOperationFactory(startTimes, dependencyTimes);

        Iterator<Operation<?>> operations = gf.limit(timedNothingOperations, operationCount);

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, null));

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        Workload workload = new DummyWorkload(operations, operationClassifications, maxExpectedInterleave);
        workload.init(configuration);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workload, configuration);

        System.out.println(result.errorMessage());
        assertThat(result.resultType(), is(ResultType.OPERATION_CLASSIFICATION_HAS_NO_GCT_MODE));
        assertThat(result.isSuccessful(), is(false));
    }

    @Test
    public void shouldPassWhenAllOperationsHaveStartTimesAllOperationsHaveClassificationsAndMaxInterleaveIsNotExceeded()
            throws DriverConfigurationException, WorkloadException {
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Time startTime = Time.fromMilli(0);
        long operationCount = 1000;
        Iterator<Time> startTimes = gf.constantIncrementTime(startTime, Duration.fromMilli(10));
        Iterator<Time> dependencyTimes = gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0));
        Iterator<Operation<?>> timedNothingOperations = new TimedNothingOperationFactory(startTimes, dependencyTimes);

        Iterator<Operation<?>> operations = gf.limit(timedNothingOperations, operationCount);

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.NONE));


        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        Workload workload = new DummyWorkload(operations, operationClassifications, maxExpectedInterleave);
        workload.init(configuration);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workload, configuration);

        System.out.println(result.errorMessage());
        assertThat(result.resultType(), is(ResultType.SUCCESSFUL));
        assertThat(result.isSuccessful(), is(true));
    }

    @Test
    public void shouldFailWhenAllOperationsHaveStartTimesAndAllOperationsHaveClassificationsButMaxInterleaveIsExceeded()
            throws DriverConfigurationException, WorkloadException {
        Time dependencyTime = Time.fromMilli(0);
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Time startTime = Time.fromMilli(0);
        long operationCount = 1000;
        Iterator<Time> startTimes = gf.constantIncrementTime(startTime, Duration.fromMilli(10));
        Iterator<Time> dependencyTimes = gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0));
        Iterator<Operation<?>> timedNothingOperations = new TimedNothingOperationFactory(startTimes, dependencyTimes);

        List<Operation<?>> operations = Lists.newArrayList(gf.limit(timedNothingOperations, operationCount - 1));

        Time lastOperationStartTime = operations.get(operations.size() - 1).scheduledStartTime();
        Duration excessiveDurationBetweenOperations = maxExpectedInterleave.plus(Duration.fromMilli(1));
        operations.add(new TimedOperation(lastOperationStartTime.plus(excessiveDurationBetweenOperations), dependencyTime));

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.NONE));

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        Workload workload = new DummyWorkload(operations.iterator(), operationClassifications, maxExpectedInterleave);
        workload.init(configuration);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workload, configuration);

        System.out.println(result.errorMessage());
        assertThat(result.resultType(), is(ResultType.SCHEDULED_START_TIME_INTERVAL_EXCEEDS_MAXIMUM));
        assertThat(result.isSuccessful(), is(false));
    }

    @Test
    public void shouldFailWhenAllOperationsHaveStartTimesMaxInterleaveIsNotExceededButSomeOperationsHaveNoClassification()
            throws DriverConfigurationException, WorkloadException {
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Time startTime = Time.fromMilli(0);
        long operationCount = 1000;
        Iterator<Time> startTimes = gf.constantIncrementTime(startTime, Duration.fromMilli(10));
        Iterator<Time> dependencyTimes = gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0));
        Iterator<Operation<?>> timedNothingOperations = new TimedNothingOperationFactory(startTimes, dependencyTimes);

        List<Operation<?>> operations = Lists.newArrayList(gf.limit(timedNothingOperations, operationCount));

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        Workload workload = new DummyWorkload(operations.iterator(), operationClassifications, maxExpectedInterleave);
        workload.init(configuration);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workload, configuration);

        System.out.println(result.errorMessage());
        assertThat(result.resultType(), is(ResultType.OPERATION_HAS_NO_CLASSIFICATION));
        assertThat(result.isSuccessful(), is(false));
    }

    @Test
    public void shouldFailWhenAllOperationsHaveClassificationsAndMaxInterleaveIsNotExceededButSomeOperationsHaveNoStartTime()
            throws DriverConfigurationException, WorkloadException {
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Time startTime = Time.fromMilli(0);
        long operationCount = 1000;
        Iterator<Time> startTimes = gf.constantIncrementTime(startTime, Duration.fromMilli(10));
        Iterator<Time> dependencyTimes = gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0));
        Iterator<Operation<?>> timedNothingOperations = new TimedNothingOperationFactory(startTimes, dependencyTimes);

        List<Operation<?>> operations = Lists.newArrayList(gf.limit(timedNothingOperations, operationCount - 1));
        operations.add(new NothingOperation());

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.NONE));
        operationClassifications.put(
                NothingOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.NONE));

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        Workload workload = new DummyWorkload(operations.iterator(), operationClassifications, maxExpectedInterleave);
        workload.init(configuration);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workload, configuration);

        System.out.println(result.errorMessage());
        assertThat(result.resultType(), is(ResultType.UNASSIGNED_SCHEDULED_START_TIME));
        assertThat(result.isSuccessful(), is(false));
    }

    @Test
    public void shouldFailWhenMaxInterleaveIsNotExceededButSomeOperationsHaveNoStartTimeAndOperationsHaveNoClassification()
            throws DriverConfigurationException, WorkloadException {
        Time startTime = Time.fromMilli(0);
        long operationCount = 1000;
        Iterator<Time> startTimes = gf.constantIncrementTime(startTime, Duration.fromMilli(10));
        Iterator<Time> dependencyTimes = gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0));
        Iterator<Operation<?>> timedNothingOperations = new TimedNothingOperationFactory(startTimes, dependencyTimes);

        List<Operation<?>> operations = Lists.newArrayList(gf.limit(timedNothingOperations, operationCount - 1));
        operations.add(new NothingOperation());

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.NONE));

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        Workload workload = new DummyWorkload(operations.iterator(), operationClassifications, Workload.DEFAULT_MAXIMUM_EXPECTED_INTERLEAVE);
        workload.init(configuration);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workload, configuration);

        System.out.println(result.errorMessage());
        assertThat(result.resultType(), anyOf(is(ResultType.UNASSIGNED_SCHEDULED_START_TIME), is(ResultType.OPERATION_HAS_NO_CLASSIFICATION)));
        assertThat(result.isSuccessful(), is(false));
    }

    @Test
    public void shouldFailWhenOperationStartTimesAreNotMonotonicallyIncreasing()
            throws DriverConfigurationException, WorkloadException {
        Time dependencyTime = Time.fromMilli(0);
        Time startTime = Time.fromMilli(0);
        long operationCount = 1000;
        Iterator<Time> startTimes = gf.constantIncrementTime(startTime, Duration.fromMilli(10));
        Iterator<Time> dependencyTimes = gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0));
        Iterator<Operation<?>> timedNothingOperations = new TimedNothingOperationFactory(startTimes, dependencyTimes);

        List<Operation<?>> operations = Lists.newArrayList(gf.limit(timedNothingOperations, operationCount - 1));
        Time slightlyBeforeLastOperationStartTime = operations.get(operations.size() - 1).scheduledStartTime().minus(Duration.fromMilli(1));
        operations.add(new TimedOperation(slightlyBeforeLastOperationStartTime, dependencyTime));

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.NONE));

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        Workload workload = new DummyWorkload(operations.iterator(), operationClassifications, Workload.DEFAULT_MAXIMUM_EXPECTED_INTERLEAVE);
        workload.init(configuration);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workload, configuration);

        System.out.println(result.errorMessage());
        assertThat(result.resultType(), is(ResultType.SCHEDULED_START_TIMES_DO_NOT_INCREASE_MONOTONICALLY));
        assertThat(result.isSuccessful(), is(false));
    }

    @Test
    public void shouldFailWhenSomeOperationClassificationsDoNotContainSchedulingMode()
            throws DriverConfigurationException, WorkloadException {
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Time startTime = Time.fromMilli(0);
        long operationCount = 1000;
        Iterator<Time> startTimes = gf.constantIncrementTime(startTime, Duration.fromMilli(10));
        Iterator<Time> dependencyTimes = gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0));
        Iterator<Operation<?>> timedNothingOperations = new TimedNothingOperationFactory(startTimes, dependencyTimes);

        Iterator<Operation<?>> operations = gf.limit(timedNothingOperations, operationCount);

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedOperation.class,
                new OperationClassification(null, OperationClassification.GctMode.NONE));

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        Workload workload = new DummyWorkload(operations, operationClassifications, maxExpectedInterleave);
        workload.init(configuration);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workload, configuration);

        System.out.println(result.errorMessage());
        assertThat(result.resultType(), is(ResultType.OPERATION_CLASSIFICATION_HAS_NO_SCHEDULING_MODE));
        assertThat(result.isSuccessful(), is(false));
    }
}