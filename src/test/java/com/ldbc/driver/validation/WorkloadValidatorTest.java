package com.ldbc.driver.validation;

import com.google.common.collect.Iterators;
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
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.workloads.dummy.*;
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

        List<Iterator<Operation<?>>> validOperations = Lists.newArrayList(
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(3), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(3), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(3), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(3), "name1")
                ).iterator()
        );

        List<Iterator<Operation<?>>> invalidOperations = Lists.newArrayList(
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(12), Time.fromMilli(3), "name2")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(12), Time.fromMilli(3), "name2")
                ).iterator()
        );

        long operationCount = validOperations.size();

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedNamedOperation2.class,
                new OperationClassification(OperationClassification.SchedulingMode.WINDOWED, OperationClassification.DependencyMode.READ));
        operationClassifications.put(
                TimedNamedOperation1.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));

        Map<String, String> nonDefaultParams = new HashMap<>();
        nonDefaultParams.put(ConsoleAndFileDriverConfiguration.WINDOWED_EXECUTION_WINDOW_DURATION_ARG, Long.toString(windowDuration.asMilli()));
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount).applyMap(nonDefaultParams);

        WorkloadFactory validWorkloadFactory = new DummyWorkloadFactory(validOperations.iterator(), operationClassifications, maxExpectedInterleave);
        WorkloadFactory invalidWorkloadFactory = new DummyWorkloadFactory(invalidOperations.iterator(), operationClassifications, maxExpectedInterleave);

        // When
        WorkloadValidator validWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult validResult = validWorkloadValidator.validate(validWorkloadFactory, configuration);

        WorkloadValidator invalidWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult invalidResult = invalidWorkloadValidator.validate(invalidWorkloadFactory, configuration);

        // Then
        assertThat(validResult.errorMessage(), validResult.resultType(), is(ResultType.SUCCESSFUL));
        assertThat(validResult.errorMessage(), validResult.isSuccessful(), is(true));

        assertThat(invalidResult.errorMessage(), invalidResult.resultType(), is(ResultType.INSUFFICIENT_INTERVAL_BETWEEN_DEPENDENCY_TIME_AND_SCHEDULED_START_TIME));
        assertThat(invalidResult.errorMessage(), invalidResult.isSuccessful(), is(false));
    }

    @Test
    public void shouldTestThatAllOperationsHaveDependencyTimeSet() throws DriverConfigurationException, WorkloadException {
        // Given
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Duration windowDuration = Duration.fromMilli(10);

        List<Iterator<Operation<?>>> validOperations = Lists.newArrayList(
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );

        List<Iterator<Operation<?>>> invalidOperations = Lists.<Iterator<Operation<?>>>newArrayList(
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), null, "name1")
                ).iterator()
        );

        long operationCount = validOperations.size();

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedNamedOperation2.class,
                new OperationClassification(OperationClassification.SchedulingMode.WINDOWED, OperationClassification.DependencyMode.READ));
        operationClassifications.put(
                TimedNamedOperation1.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));

        Map<String, String> nonDefaultParams = new HashMap<>();
        nonDefaultParams.put(ConsoleAndFileDriverConfiguration.WINDOWED_EXECUTION_WINDOW_DURATION_ARG, Long.toString(windowDuration.asMilli()));
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount).applyMap(nonDefaultParams);

        WorkloadFactory validWorkloadFactory = new DummyWorkloadFactory(validOperations.iterator(), operationClassifications, maxExpectedInterleave);
        WorkloadFactory invalidWorkloadFactory = new DummyWorkloadFactory(invalidOperations.iterator(), operationClassifications, maxExpectedInterleave);

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
        Duration windowDuration = Duration.fromMilli(10);

        List<Iterator<Operation<?>>> validOperations = Lists.newArrayList(
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(11), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(11), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(11), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(11), "name1")
                ).iterator()
        );

        List<Iterator<Operation<?>>> invalidOperations = Lists.<Iterator<Operation<?>>>newArrayList(
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(13), "name1")
                ).iterator()
        );

        long operationCount = validOperations.size();

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedNamedOperation2.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.READ));
        operationClassifications.put(
                TimedNamedOperation1.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));

        Map<String, String> nonDefaultParams = new HashMap<>();
        nonDefaultParams.put(ConsoleAndFileDriverConfiguration.WINDOWED_EXECUTION_WINDOW_DURATION_ARG, Long.toString(windowDuration.asMilli()));
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount).applyMap(nonDefaultParams);

        WorkloadFactory validWorkloadFactory = new DummyWorkloadFactory(validOperations.iterator(), operationClassifications, maxExpectedInterleave);
        WorkloadFactory invalidWorkloadFactory = new DummyWorkloadFactory(invalidOperations.iterator(), operationClassifications, maxExpectedInterleave);

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
        Duration windowDuration = Duration.fromMilli(10);

        List<Iterator<Operation<?>>> validOperations = Lists.newArrayList(
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );

        List<Iterator<Operation<?>>> invalidOperations = Lists.newArrayList(
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(12), Time.fromMilli(2), "name2")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(12), Time.fromMilli(2), "name2")
                ).iterator()
        );

        long operationCount = validOperations.size();

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedNamedOperation2.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.READ));
        operationClassifications.put(
                TimedNamedOperation1.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));

        Map<String, String> nonDefaultParams = new HashMap<>();
        nonDefaultParams.put(ConsoleAndFileDriverConfiguration.WINDOWED_EXECUTION_WINDOW_DURATION_ARG, Long.toString(windowDuration.asMilli()));
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount).applyMap(nonDefaultParams);

        WorkloadFactory validWorkloadFactory = new DummyWorkloadFactory(validOperations.iterator(), operationClassifications, maxExpectedInterleave);
        WorkloadFactory invalidWorkloadFactory = new DummyWorkloadFactory(invalidOperations.iterator(), operationClassifications, maxExpectedInterleave);

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

        List<Iterator<Operation<?>>> baseOperations1 = Lists.newArrayList(
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );

        List<Iterator<Operation<?>>> baseOperations2 = Lists.newArrayList(
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
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

        long operationCount = baseOperations1.size();

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedNamedOperation2.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.READ));
        operationClassifications.put(
                TimedNamedOperation1.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));

        Map<String, String> nonDefaultParams = new HashMap<>();
        nonDefaultParams.put(ConsoleAndFileDriverConfiguration.WINDOWED_EXECUTION_WINDOW_DURATION_ARG, Long.toString(windowDuration.asMilli()));
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount).applyMap(nonDefaultParams);

        WorkloadFactory validWorkloadFactory = new DummyWorkloadFactory(baseOperations1.iterator(), validAlternativeOperations.iterator(), operationClassifications, maxExpectedInterleave);
        WorkloadFactory invalidWorkloadFactory = new DummyWorkloadFactory(baseOperations2.iterator(), invalidAlternativeOperations.iterator(), operationClassifications, maxExpectedInterleave);

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
    public void shouldTestThatAllOperationClassificationsContainSchedulingMode() throws DriverConfigurationException, WorkloadException {
        // Given
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Duration windowDuration = Duration.fromMilli(10);

        List<Iterator<Operation<?>>> validOperations = Lists.newArrayList(
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );

        List<Iterator<Operation<?>>> invalidOperations = Lists.newArrayList(
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator(),
                Lists.<Operation<?>>newArrayList(
                        new TimedNamedOperation2(Time.fromMilli(10), Time.fromMilli(0), "name2"),
                        new TimedNamedOperation2(Time.fromMilli(11), Time.fromMilli(1), "name2"),
                        new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(2), "name1")
                ).iterator()
        );
        long operationCount = validOperations.size();

        Map<Class<? extends Operation>, OperationClassification> validOperationClassifications = new HashMap<>();
        validOperationClassifications.put(
                TimedNamedOperation2.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.READ));
        validOperationClassifications.put(
                TimedNamedOperation1.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));

        Map<Class<? extends Operation>, OperationClassification> invalidOperationClassifications = new HashMap<>();
        invalidOperationClassifications.put(
                TimedNamedOperation2.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.READ));
        invalidOperationClassifications.put(
                TimedNamedOperation1.class,
                new OperationClassification(null, OperationClassification.DependencyMode.NONE));

        Map<String, String> nonDefaultParams = new HashMap<>();
        nonDefaultParams.put(ConsoleAndFileDriverConfiguration.WINDOWED_EXECUTION_WINDOW_DURATION_ARG, Long.toString(windowDuration.asMilli()));
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount).applyMap(nonDefaultParams);

        WorkloadFactory validWorkloadFactory = new DummyWorkloadFactory(validOperations.iterator(), validOperationClassifications, maxExpectedInterleave);
        WorkloadFactory invalidWorkloadFactory = new DummyWorkloadFactory(invalidOperations.iterator(), invalidOperationClassifications, maxExpectedInterleave);

        // When
        WorkloadValidator validWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult validResult = validWorkloadValidator.validate(validWorkloadFactory, configuration);

        WorkloadValidator invalidWorkloadValidator = new WorkloadValidator();
        WorkloadValidationResult invalidResult = invalidWorkloadValidator.validate(invalidWorkloadFactory, configuration);

        // Then
        assertThat(validResult.errorMessage(), validResult.resultType(), is(ResultType.SUCCESSFUL));
        assertThat(validResult.errorMessage(), validResult.isSuccessful(), is(true));

        assertThat(invalidResult.errorMessage(), invalidResult.resultType(), is(ResultType.OPERATION_CLASSIFICATION_HAS_NO_SCHEDULING_MODE));
        assertThat(invalidResult.errorMessage(), invalidResult.isSuccessful(), is(false));
    }

    @Test
    public void shouldTestThatAllOperationClassificationsContainGctMode()
            throws DriverConfigurationException, WorkloadException {
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Time startTime = Time.fromMilli(0);
        long operationCount = 1000;

        List<Iterator<Operation<?>>> operations = Lists.<Iterator<Operation<?>>>newArrayList(
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                gf.constant("name")),
                        operationCount
                ),
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                gf.constant("name")),
                        operationCount
                )
        );

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedNamedOperation1.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, null));

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        WorkloadFactory workloadFactory = new DummyWorkloadFactory(operations.iterator(), operationClassifications, maxExpectedInterleave);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workloadFactory, configuration);

        assertThat(result.errorMessage(), result.resultType(), is(ResultType.OPERATION_CLASSIFICATION_HAS_NO_GCT_MODE));
        assertThat(result.errorMessage(), result.isSuccessful(), is(false));
    }

    @Test
    public void shouldPassWhenAllOperationsHaveStartTimesAllOperationsHaveClassificationsAndMaxInterleaveIsNotExceeded()
            throws DriverConfigurationException, WorkloadException {
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Time startTime = Time.fromMilli(0);
        long operationCount = 1000;
        List<Iterator<Operation<?>>> operations = Lists.newArrayList(
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                gf.constant("name")
                        ),
                        operationCount
                ),
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                gf.constant("name")
                        ),
                        operationCount
                ),
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                gf.constant("name")
                        ),
                        operationCount
                ),
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                gf.constant("name")
                        ),
                        operationCount
                )
        );

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedNamedOperation1.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));


        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        WorkloadFactory workloadFactory = new DummyWorkloadFactory(operations.iterator(), operationClassifications, maxExpectedInterleave);
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

        List<Iterator<Operation<?>>> operations = Lists.<Iterator<Operation<?>>>newArrayList(
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
                ),
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

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedNamedOperation1.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        WorkloadFactory workloadFactory = new DummyWorkloadFactory(operations.iterator(), operationClassifications, maxExpectedInterleave);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workloadFactory, configuration);

        assertThat(result.errorMessage(), result.resultType(), is(ResultType.SCHEDULED_START_TIME_INTERVAL_EXCEEDS_MAXIMUM));
        assertThat(result.errorMessage(), result.isSuccessful(), is(false));
    }

    @Test
    public void shouldFailWhenAllOperationsHaveStartTimesMaxInterleaveIsNotExceededButSomeOperationsHaveNoClassification()
            throws DriverConfigurationException, WorkloadException {
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Time startTime = Time.fromMilli(0);
        long operationCount = 1000;

        List<Iterator<Operation<?>>> operations = Lists.<Iterator<Operation<?>>>newArrayList(
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                gf.constant("name")),
                        operationCount
                ),
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                gf.constant("name")),
                        operationCount
                )
        );

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        WorkloadFactory workloadFactory = new DummyWorkloadFactory(operations.iterator(), operationClassifications, maxExpectedInterleave);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workloadFactory, configuration);

        assertThat(result.errorMessage(), result.resultType(), is(ResultType.OPERATION_HAS_NO_CLASSIFICATION));
        assertThat(result.errorMessage(), result.isSuccessful(), is(false));
    }

    @Test
    public void shouldFailWhenAllOperationsHaveClassificationsAndMaxInterleaveIsNotExceededButSomeOperationsHaveNoStartTime()
            throws DriverConfigurationException, WorkloadException {
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Time startTime = Time.fromMilli(0);
        long operationCount = 1000;

        List<Iterator<Operation<?>>> operations = Lists.<Iterator<Operation<?>>>newArrayList(
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

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedNamedOperation1.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
        operationClassifications.put(
                NothingOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        WorkloadFactory workloadFactory = new DummyWorkloadFactory(operations.iterator(), operationClassifications, maxExpectedInterleave);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workloadFactory, configuration);

        assertThat(result.errorMessage(), result.resultType(), is(ResultType.UNASSIGNED_SCHEDULED_START_TIME));
        assertThat(result.errorMessage(), result.isSuccessful(), is(false));
    }

    @Test
    public void shouldFailWhenMaxInterleaveIsNotExceededButSomeOperationsHaveNoStartTimeAndOperationsHaveNoClassification()
            throws DriverConfigurationException, WorkloadException {
        Time startTime = Time.fromMilli(0);
        long operationCount = 1000;

        List<Iterator<Operation<?>>> operations = Lists.<Iterator<Operation<?>>>newArrayList(
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

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedNamedOperation1.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        WorkloadFactory workloadFactory = new DummyWorkloadFactory(operations.iterator(), operationClassifications, Workload.DEFAULT_MAXIMUM_EXPECTED_INTERLEAVE);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workloadFactory, configuration);

        assertThat(result.errorMessage(), result.resultType(), anyOf(is(ResultType.UNASSIGNED_SCHEDULED_START_TIME), is(ResultType.OPERATION_HAS_NO_CLASSIFICATION)));
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

        List<Iterator<Operation<?>>> operations = Lists.<Iterator<Operation<?>>>newArrayList(
                Iterators.concat(
                        gf.limit(
                                new TimedNamedOperation1Factory(
                                        gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                        gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                        gf.constant("name")),
                                operationCount - 1
                        ),
                        gf.identity(new TimedNamedOperation1(slightlyBeforeLastOperationStartTime, Time.fromMilli(0), "name"))
                ),
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

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedNamedOperation1.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        WorkloadFactory workloadFactory = new DummyWorkloadFactory(operations.iterator(), operationClassifications, Workload.DEFAULT_MAXIMUM_EXPECTED_INTERLEAVE);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workloadFactory, configuration);

        assertThat(result.errorMessage(), result.resultType(), is(ResultType.SCHEDULED_START_TIMES_DO_NOT_INCREASE_MONOTONICALLY));
        assertThat(result.errorMessage(), result.isSuccessful(), is(false));
    }

    @Test
    public void shouldFailWhenSomeOperationClassificationsDoNotContainSchedulingMode()
            throws DriverConfigurationException, WorkloadException {
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Time startTime = Time.fromMilli(0);
        long operationCount = 1000;

        List<Iterator<Operation<?>>> operations = Lists.<Iterator<Operation<?>>>newArrayList(
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                gf.constant("name")),
                        operationCount
                ),
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(startTime, Duration.fromMilli(10)),
                                gf.constantIncrementTime(startTime.minus(Duration.fromMilli(1)), Duration.fromMilli(0)),
                                gf.constant("name")),
                        operationCount
                )
        );

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedNamedOperation1.class,
                new OperationClassification(null, OperationClassification.DependencyMode.NONE));

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        WorkloadFactory workloadFactory = new DummyWorkloadFactory(operations.iterator(), operationClassifications, maxExpectedInterleave);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workloadFactory, configuration);

        assertThat(result.errorMessage(), result.resultType(), is(ResultType.OPERATION_CLASSIFICATION_HAS_NO_SCHEDULING_MODE));
        assertThat(result.errorMessage(), result.isSuccessful(), is(false));
    }
}