package com.ldbc.driver.validation;

import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.testutils.NothingOperation;
import com.ldbc.driver.testutils.TimedNothingOperation;
import com.ldbc.driver.testutils.TimedNothingOperationFactory;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WorkloadValidatorTest {
    TimeSource TIME_SOURCE = new SystemTimeSource();
    GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));

    @Test
    public void shouldPassWhenAllOperationsHaveStartTimesAllOperationsHaveClassificationsAndMaxInterleaveIsNotExceeded()
            throws MetricsCollectionException, DriverConfigurationException, WorkloadException {
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Time startTime = TIME_SOURCE.now();
        long operationCount = 1000;
        Iterator<Time> startTimes = gf.constantIncrementTime(startTime, Duration.fromMilli(10));
        Iterator<Operation<?>> timedNothingOperations = (Iterator) new TimedNothingOperationFactory(startTimes);

        Iterator<Operation<?>> operations = gf.limit(timedNothingOperations, operationCount);

        Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedNothingOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.NONE));


        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        Workload workload = new DummyWorkload(operations, operationClassifications, maxExpectedInterleave);
        workload.init(configuration);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workload, configuration);

        assertThat(result.isSuccessful(), is(true));
    }

    @Test
    public void shouldFailWhenAllOperationsHaveStartTimesAllOperationsHaveClassificationsButMaxInterleaveIsExceeded()
            throws MetricsCollectionException, DriverConfigurationException, WorkloadException {
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Time startTime = TIME_SOURCE.now();
        long operationCount = 1000;
        Iterator<Time> startTimes = gf.constantIncrementTime(startTime, Duration.fromMilli(10));
        Iterator<Operation<?>> timedNothingOperations = (Iterator) new TimedNothingOperationFactory(startTimes);

        List<Operation<?>> operations = Lists.newArrayList(gf.limit(timedNothingOperations, operationCount - 1));

        Time lastOperationStartTime = operations.get(operations.size() - 1).scheduledStartTime();
        Duration excessiveDurationBetweenOperations = maxExpectedInterleave.plus(Duration.fromMilli(1));
        operations.add(new TimedNothingOperation(lastOperationStartTime.plus(excessiveDurationBetweenOperations)));

        Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedNothingOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.NONE));

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        Workload workload = new DummyWorkload(operations.iterator(), operationClassifications, maxExpectedInterleave);
        workload.init(configuration);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workload, configuration);

        assertThat(result.isSuccessful(), is(false));
    }

    @Test
    public void shouldFailWhenAllOperationsHaveStartTimesMaxInterleaveIsNotExceededButSomeOperationsHaveNoClassification()
            throws MetricsCollectionException, DriverConfigurationException, WorkloadException {
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Time startTime = TIME_SOURCE.now();
        long operationCount = 1000;
        Iterator<Time> startTimes = gf.constantIncrementTime(startTime, Duration.fromMilli(10));
        Iterator<Operation<?>> timedNothingOperations = (Iterator) new TimedNothingOperationFactory(startTimes);

        List<Operation<?>> operations = Lists.newArrayList(gf.limit(timedNothingOperations, operationCount));

        Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications = new HashMap<>();

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        Workload workload = new DummyWorkload(operations.iterator(), operationClassifications, maxExpectedInterleave);
        workload.init(configuration);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workload, configuration);

        assertThat(result.isSuccessful(), is(false));
    }

    @Test
    public void shouldFailWhenAllOperationsHaveClassificationsAndMaxInterleaveIsNotExceededButSomeOperationsHaveNoStartTime()
            throws MetricsCollectionException, DriverConfigurationException, WorkloadException {
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Time startTime = TIME_SOURCE.now();
        long operationCount = 1000;
        Iterator<Time> startTimes = gf.constantIncrementTime(startTime, Duration.fromMilli(10));
        Iterator<Operation<?>> timedNothingOperations = (Iterator) new TimedNothingOperationFactory(startTimes);

        List<Operation<?>> operations = Lists.newArrayList(gf.limit(timedNothingOperations, operationCount - 1));
        operations.add(new NothingOperation());

        Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedNothingOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.NONE));
        operationClassifications.put(
                NothingOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.NONE));

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        Workload workload = new DummyWorkload(operations.iterator(), operationClassifications, maxExpectedInterleave);
        workload.init(configuration);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workload, configuration);

        assertThat(result.isSuccessful(), is(false));
    }

    @Test
    public void shouldFailWhenMaxInterleaveIsNotExceededButSomeOperationsHaveNoStartTimeAndOperationsHaveNoClassification() throws MetricsCollectionException, DriverConfigurationException, WorkloadException {
        Time startTime = TIME_SOURCE.now();
        long operationCount = 1000;
        Iterator<Time> startTimes = gf.constantIncrementTime(startTime, Duration.fromMilli(10));
        Iterator<Operation<?>> timedNothingOperations = (Iterator) new TimedNothingOperationFactory(startTimes);

        List<Operation<?>> operations = Lists.newArrayList(gf.limit(timedNothingOperations, operationCount - 1));
        operations.add(new NothingOperation());

        Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedNothingOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.NONE));

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        Workload workload = new DummyWorkload(operations.iterator(), operationClassifications, Workload.DEFAULT_MAXIMUM_EXPECTED_INTERLEAVE);
        workload.init(configuration);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workload, configuration);

        assertThat(result.isSuccessful(), is(false));
    }

    @Test
    public void shouldFailWhenOperationStartTimesAreNotMonotonicallyIncreasing() throws MetricsCollectionException, DriverConfigurationException, WorkloadException {
        Time startTime = TIME_SOURCE.now();
        long operationCount = 1000;
        Iterator<Time> startTimes = gf.constantIncrementTime(startTime, Duration.fromMilli(10));
        Iterator<Operation<?>> timedNothingOperations = (Iterator) new TimedNothingOperationFactory(startTimes);

        List<Operation<?>> operations = Lists.newArrayList(gf.limit(timedNothingOperations, operationCount - 1));
        Time slightlyBeforeLastOperationStartTime = operations.get(operations.size() - 1).scheduledStartTime().minus(Duration.fromMilli(1));
        operations.add(new TimedNothingOperation(slightlyBeforeLastOperationStartTime));

        Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedNothingOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.NONE));

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        Workload workload = new DummyWorkload(operations.iterator(), operationClassifications, Workload.DEFAULT_MAXIMUM_EXPECTED_INTERLEAVE);
        workload.init(configuration);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workload, configuration);

        assertThat(result.isSuccessful(), is(false));
    }

    @Ignore
    @Test
    public void shouldTestForDeterminism() throws MetricsCollectionException {
        assertThat(true, is(false));
        assertThat(true, equalTo(false));
    }

    @Test
    public void shouldFailWhenSomeOperationClassificationsDoNotContainGctMode() throws MetricsCollectionException, DriverConfigurationException, WorkloadException {
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Time startTime = TIME_SOURCE.now();
        long operationCount = 1000;
        Iterator<Time> startTimes = gf.constantIncrementTime(startTime, Duration.fromMilli(10));
        Iterator<Operation<?>> timedNothingOperations = (Iterator) new TimedNothingOperationFactory(startTimes);

        Iterator<Operation<?>> operations = gf.limit(timedNothingOperations, operationCount);

        Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedNothingOperation.class,
                new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, null));

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        Workload workload = new DummyWorkload(operations, operationClassifications, maxExpectedInterleave);
        workload.init(configuration);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workload, configuration);

        assertThat(result.isSuccessful(), is(false));
    }

    @Test
    public void shouldFailWhenSomeOperationClassificationsDoNotContainSchedulingMode() throws MetricsCollectionException, DriverConfigurationException, WorkloadException {
        Duration maxExpectedInterleave = Duration.fromMilli(1000);
        Time startTime = TIME_SOURCE.now();
        long operationCount = 1000;
        Iterator<Time> startTimes = gf.constantIncrementTime(startTime, Duration.fromMilli(10));
        Iterator<Operation<?>> timedNothingOperations = (Iterator) new TimedNothingOperationFactory(startTimes);

        Iterator<Operation<?>> operations = gf.limit(timedNothingOperations, operationCount);

        Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(
                TimedNothingOperation.class,
                new OperationClassification(null, OperationClassification.GctMode.NONE));

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, operationCount);
        Workload workload = new DummyWorkload(operations, operationClassifications, maxExpectedInterleave);
        workload.init(configuration);
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult result = workloadValidator.validate(workload, configuration);

        assertThat(result.isSuccessful(), is(false));
    }

    @Ignore
    @Test
    public void shouldTestThatAllOperationClassificationsContainSchedulingMode() throws MetricsCollectionException {
        assertThat(true, is(false));
        assertThat(true, equalTo(false));
    }

    private class DummyWorkload extends Workload {
        private final List<Operation<?>> operations;
        private final Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications;
        private final Duration maxExpectedInterleave;

        DummyWorkload(Iterator<Operation<?>> operations,
                      Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications,
                      Duration maxExpectedInterleave) {
            this.operations = Lists.newArrayList(operations);
            this.operationClassifications = operationClassifications;
            this.maxExpectedInterleave = maxExpectedInterleave;
        }

        @Override
        public Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications() {
            return operationClassifications;
        }

        @Override
        public void onInit(Map<String, String> params) throws WorkloadException {
        }

        @Override
        protected void onCleanup() throws WorkloadException {
        }

        @Override
        protected Iterator<Operation<?>> createOperations(GeneratorFactory generators) throws WorkloadException {
            return operations.iterator();
        }

        @Override
        public String serializeOperation(Operation<?> operation) throws SerializingMarshallingException {
            if (operation.getClass().equals(NothingOperation.class)) return NothingOperation.class.getName();
            if (operation.getClass().equals(TimedNothingOperation.class))
                return TimedNothingOperation.class.getName() + Long.toString(operation.scheduledStartTime().asMilli());
            throw new SerializingMarshallingException("Unsupported Operation: " + operation.getClass().getName());
        }

        @Override
        public Operation<?> marshalOperation(String serializedOperation) throws SerializingMarshallingException {
            if (serializedOperation.startsWith(NothingOperation.class.getName())) return new NothingOperation();
            if (serializedOperation.startsWith(TimedNothingOperation.class.getName())) {
                String timeAsMillString = serializedOperation.substring(TimedNothingOperation.class.getName().length(), serializedOperation.length());
                return new TimedNothingOperation(Time.fromMilli(Long.parseLong(timeAsMillString)));
            }
            throw new SerializingMarshallingException("Unsupported Operation: " + serializedOperation);
        }

        @Override
        public Duration maxExpectedInterleave() {
            return maxExpectedInterleave;
        }
    }
}