package com.ldbc.driver;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.validation.WorkloadFactory;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1Factory;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation2Factory;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation3Factory;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class WorkloadStreamsTest {

    @Test
    public void shouldReturnSameWorkloadStreamsAsCreatedWith() {
        WorkloadStreams workloadStreamsBefore = getWorkloadStreams();

        Operation<?> firstAsyncDependencyOperation = workloadStreamsBefore.asynchronousStream().dependencyOperations().next();
        Operation<?> secondAsyncDependencyOperation = workloadStreamsBefore.asynchronousStream().dependencyOperations().next();
        assertThat(firstAsyncDependencyOperation.scheduledStartTime(), is(Time.fromNano(0)));
        assertThat(secondAsyncDependencyOperation.scheduledStartTime(), is(Time.fromNano(10)));
        assertThat(secondAsyncDependencyOperation.scheduledStartTime().durationGreaterThan(firstAsyncDependencyOperation.scheduledStartTime()), is(Duration.fromNano(10)));
        assertThat(secondAsyncDependencyOperation.dependencyTime().durationGreaterThan(firstAsyncDependencyOperation.dependencyTime()), is(Duration.fromNano(10)));

        Operation<?> firstAsyncNonDependencyOperation = workloadStreamsBefore.asynchronousStream().nonDependencyOperations().next();
        Operation<?> secondAsyncNonDependencyOperation = workloadStreamsBefore.asynchronousStream().nonDependencyOperations().next();
        assertThat(firstAsyncNonDependencyOperation.scheduledStartTime(), is(Time.fromNano(2)));
        assertThat(secondAsyncNonDependencyOperation.scheduledStartTime(), is(Time.fromNano(102)));
        assertThat(secondAsyncNonDependencyOperation.scheduledStartTime().durationGreaterThan(firstAsyncNonDependencyOperation.scheduledStartTime()), is(Duration.fromNano(100)));
        assertThat(secondAsyncNonDependencyOperation.dependencyTime().durationGreaterThan(firstAsyncNonDependencyOperation.dependencyTime()), is(Duration.fromNano(100)));

        Operation<?> firstBlocking1DependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(0).dependencyOperations().next();
        Operation<?> secondBlocking1DependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(0).dependencyOperations().next();
        assertThat(firstBlocking1DependencyOperation.scheduledStartTime(), is(Time.fromNano(4)));
        assertThat(secondBlocking1DependencyOperation.scheduledStartTime(), is(Time.fromNano(1004)));
        assertThat(secondBlocking1DependencyOperation.scheduledStartTime().durationGreaterThan(firstBlocking1DependencyOperation.scheduledStartTime()), is(Duration.fromNano(1000)));
        assertThat(secondBlocking1DependencyOperation.dependencyTime().durationGreaterThan(firstBlocking1DependencyOperation.dependencyTime()), is(Duration.fromNano(1000)));

        Operation<?> firstBlocking1NonDependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(0).nonDependencyOperations().next();
        Operation<?> secondBlocking1NonDependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(0).nonDependencyOperations().next();
        assertThat(firstBlocking1NonDependencyOperation.scheduledStartTime(), is(Time.fromNano(6)));
        assertThat(secondBlocking1NonDependencyOperation.scheduledStartTime(), is(Time.fromNano(10006)));
        assertThat(secondBlocking1NonDependencyOperation.scheduledStartTime().durationGreaterThan(firstBlocking1NonDependencyOperation.scheduledStartTime()), is(Duration.fromNano(10000)));
        assertThat(secondBlocking1NonDependencyOperation.dependencyTime().durationGreaterThan(firstBlocking1NonDependencyOperation.dependencyTime()), is(Duration.fromNano(10000)));

        Operation<?> firstBlocking2DependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(1).dependencyOperations().next();
        Operation<?> secondBlocking2DependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(1).dependencyOperations().next();
        assertThat(firstBlocking2DependencyOperation.scheduledStartTime(), is(Time.fromNano(8)));
        assertThat(secondBlocking2DependencyOperation.scheduledStartTime(), is(Time.fromNano(10008)));
        assertThat(secondBlocking2DependencyOperation.scheduledStartTime().durationGreaterThan(firstBlocking2DependencyOperation.scheduledStartTime()), is(Duration.fromNano(10000)));
        assertThat(secondBlocking2DependencyOperation.dependencyTime().durationGreaterThan(firstBlocking2DependencyOperation.dependencyTime()), is(Duration.fromNano(10000)));

        Operation<?> firstBlocking2NonDependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(1).nonDependencyOperations().next();
        Operation<?> secondBlocking2NonDependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(1).nonDependencyOperations().next();
        assertThat(firstBlocking2NonDependencyOperation.scheduledStartTime(), is(Time.fromNano(10)));
        assertThat(secondBlocking2NonDependencyOperation.scheduledStartTime(), is(Time.fromNano(100010)));
        assertThat(secondBlocking2NonDependencyOperation.scheduledStartTime().durationGreaterThan(firstBlocking2NonDependencyOperation.scheduledStartTime()), is(Duration.fromNano(100000)));
        assertThat(secondBlocking2NonDependencyOperation.dependencyTime().durationGreaterThan(firstBlocking2NonDependencyOperation.dependencyTime()), is(Duration.fromNano(100000)));
    }

    @Test
    public void shouldPerformTimeOffsetCorrectly() throws WorkloadException {
        Duration offset = Duration.fromSeconds(100);
        WorkloadStreams workloadStreamsBefore = WorkloadStreams.timeOffsetAndCompressWorkloadStreams(
                getWorkloadStreams(),
                Time.fromNano(0).plus(offset),
                1.0,
                new GeneratorFactory(new RandomDataGeneratorFactory(42l)));

        Operation<?> firstAsyncDependencyOperation = workloadStreamsBefore.asynchronousStream().dependencyOperations().next();
        Operation<?> secondAsyncDependencyOperation = workloadStreamsBefore.asynchronousStream().dependencyOperations().next();
        assertThat(firstAsyncDependencyOperation.scheduledStartTime(), is(Time.fromNano(0).plus(offset)));
        assertThat(secondAsyncDependencyOperation.scheduledStartTime(), is(Time.fromNano(10).plus(offset)));
        assertThat(secondAsyncDependencyOperation.scheduledStartTime().durationGreaterThan(firstAsyncDependencyOperation.scheduledStartTime()), is(Duration.fromNano(10)));
        assertThat(secondAsyncDependencyOperation.dependencyTime().durationGreaterThan(firstAsyncDependencyOperation.dependencyTime()), is(Duration.fromNano(10)));

        Operation<?> firstAsyncNonDependencyOperation = workloadStreamsBefore.asynchronousStream().nonDependencyOperations().next();
        Operation<?> secondAsyncNonDependencyOperation = workloadStreamsBefore.asynchronousStream().nonDependencyOperations().next();
        assertThat(firstAsyncNonDependencyOperation.scheduledStartTime(), is(Time.fromNano(2).plus(offset)));
        assertThat(secondAsyncNonDependencyOperation.scheduledStartTime(), is(Time.fromNano(102).plus(offset)));
        assertThat(secondAsyncNonDependencyOperation.scheduledStartTime().durationGreaterThan(firstAsyncNonDependencyOperation.scheduledStartTime()), is(Duration.fromNano(100)));
        assertThat(secondAsyncNonDependencyOperation.dependencyTime().durationGreaterThan(firstAsyncNonDependencyOperation.dependencyTime()), is(Duration.fromNano(100)));

        Operation<?> firstBlocking1DependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(0).dependencyOperations().next();
        Operation<?> secondBlocking1DependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(0).dependencyOperations().next();
        assertThat(firstBlocking1DependencyOperation.scheduledStartTime(), is(Time.fromNano(4).plus(offset)));
        assertThat(secondBlocking1DependencyOperation.scheduledStartTime(), is(Time.fromNano(1004).plus(offset)));
        assertThat(secondBlocking1DependencyOperation.scheduledStartTime().durationGreaterThan(firstBlocking1DependencyOperation.scheduledStartTime()), is(Duration.fromNano(1000)));
        assertThat(secondBlocking1DependencyOperation.dependencyTime().durationGreaterThan(firstBlocking1DependencyOperation.dependencyTime()), is(Duration.fromNano(1000)));

        Operation<?> firstBlocking1NonDependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(0).nonDependencyOperations().next();
        Operation<?> secondBlocking1NonDependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(0).nonDependencyOperations().next();
        assertThat(firstBlocking1NonDependencyOperation.scheduledStartTime(), is(Time.fromNano(6).plus(offset)));
        assertThat(secondBlocking1NonDependencyOperation.scheduledStartTime(), is(Time.fromNano(10006).plus(offset)));
        assertThat(secondBlocking1NonDependencyOperation.scheduledStartTime().durationGreaterThan(firstBlocking1NonDependencyOperation.scheduledStartTime()), is(Duration.fromNano(10000)));
        assertThat(secondBlocking1NonDependencyOperation.dependencyTime().durationGreaterThan(firstBlocking1NonDependencyOperation.dependencyTime()), is(Duration.fromNano(10000)));

        Operation<?> firstBlocking2DependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(1).dependencyOperations().next();
        Operation<?> secondBlocking2DependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(1).dependencyOperations().next();
        assertThat(firstBlocking2DependencyOperation.scheduledStartTime(), is(Time.fromNano(8).plus(offset)));
        assertThat(secondBlocking2DependencyOperation.scheduledStartTime(), is(Time.fromNano(10008).plus(offset)));
        assertThat(secondBlocking2DependencyOperation.scheduledStartTime().durationGreaterThan(firstBlocking2DependencyOperation.scheduledStartTime()), is(Duration.fromNano(10000)));
        assertThat(secondBlocking2DependencyOperation.dependencyTime().durationGreaterThan(firstBlocking2DependencyOperation.dependencyTime()), is(Duration.fromNano(10000)));

        Operation<?> firstBlocking2NonDependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(1).nonDependencyOperations().next();
        Operation<?> secondBlocking2NonDependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(1).nonDependencyOperations().next();
        assertThat(firstBlocking2NonDependencyOperation.scheduledStartTime(), is(Time.fromNano(10).plus(offset)));
        assertThat(secondBlocking2NonDependencyOperation.scheduledStartTime(), is(Time.fromNano(100010).plus(offset)));
        assertThat(secondBlocking2NonDependencyOperation.scheduledStartTime().durationGreaterThan(firstBlocking2NonDependencyOperation.scheduledStartTime()), is(Duration.fromNano(100000)));
        assertThat(secondBlocking2NonDependencyOperation.dependencyTime().durationGreaterThan(firstBlocking2NonDependencyOperation.dependencyTime()), is(Duration.fromNano(100000)));
    }

    @Test
    public void shouldPerformTimeOffsetAndCompressionCorrectly() throws WorkloadException {
        Duration offset = Duration.fromSeconds(100);
        WorkloadStreams workloadStreamsBefore = WorkloadStreams.timeOffsetAndCompressWorkloadStreams(
                getWorkloadStreams(),
                Time.fromNano(0).plus(offset),
                0.5,
                new GeneratorFactory(new RandomDataGeneratorFactory(42l)));

        Operation<?> firstAsyncDependencyOperation = workloadStreamsBefore.asynchronousStream().dependencyOperations().next();
        Operation<?> secondAsyncDependencyOperation = workloadStreamsBefore.asynchronousStream().dependencyOperations().next();
        assertThat(firstAsyncDependencyOperation.scheduledStartTime(), is(Time.fromNano(0).plus(offset)));
        assertThat(secondAsyncDependencyOperation.scheduledStartTime(), is(Time.fromNano(5).plus(offset)));
        assertThat(secondAsyncDependencyOperation.scheduledStartTime().durationGreaterThan(firstAsyncDependencyOperation.scheduledStartTime()), is(Duration.fromNano(5)));
        assertThat(secondAsyncDependencyOperation.dependencyTime().durationGreaterThan(firstAsyncDependencyOperation.dependencyTime()), is(Duration.fromNano(5)));

        Operation<?> firstAsyncNonDependencyOperation = workloadStreamsBefore.asynchronousStream().nonDependencyOperations().next();
        Operation<?> secondAsyncNonDependencyOperation = workloadStreamsBefore.asynchronousStream().nonDependencyOperations().next();
        assertThat(firstAsyncNonDependencyOperation.scheduledStartTime(), is(Time.fromNano(1).plus(offset)));
        assertThat(secondAsyncNonDependencyOperation.scheduledStartTime(), is(Time.fromNano(51).plus(offset)));
        assertThat(secondAsyncNonDependencyOperation.scheduledStartTime().durationGreaterThan(firstAsyncNonDependencyOperation.scheduledStartTime()), is(Duration.fromNano(50)));
        assertThat(secondAsyncNonDependencyOperation.dependencyTime().durationGreaterThan(firstAsyncNonDependencyOperation.dependencyTime()), is(Duration.fromNano(50)));

        Operation<?> firstBlocking1DependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(0).dependencyOperations().next();
        Operation<?> secondBlocking1DependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(0).dependencyOperations().next();
        assertThat(firstBlocking1DependencyOperation.scheduledStartTime(), is(Time.fromNano(2).plus(offset)));
        assertThat(secondBlocking1DependencyOperation.scheduledStartTime(), is(Time.fromNano(502).plus(offset)));
        assertThat(secondBlocking1DependencyOperation.scheduledStartTime().durationGreaterThan(firstBlocking1DependencyOperation.scheduledStartTime()), is(Duration.fromNano(500)));
        assertThat(secondBlocking1DependencyOperation.dependencyTime().durationGreaterThan(firstBlocking1DependencyOperation.dependencyTime()), is(Duration.fromNano(500)));

        Operation<?> firstBlocking1NonDependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(0).nonDependencyOperations().next();
        Operation<?> secondBlocking1NonDependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(0).nonDependencyOperations().next();
        assertThat(firstBlocking1NonDependencyOperation.scheduledStartTime(), is(Time.fromNano(3).plus(offset)));
        assertThat(secondBlocking1NonDependencyOperation.scheduledStartTime(), is(Time.fromNano(5003).plus(offset)));
        assertThat(secondBlocking1NonDependencyOperation.scheduledStartTime().durationGreaterThan(firstBlocking1NonDependencyOperation.scheduledStartTime()), is(Duration.fromNano(5000)));
        assertThat(secondBlocking1NonDependencyOperation.dependencyTime().durationGreaterThan(firstBlocking1NonDependencyOperation.dependencyTime()), is(Duration.fromNano(5000)));

        Operation<?> firstBlocking2DependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(1).dependencyOperations().next();
        Operation<?> secondBlocking2DependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(1).dependencyOperations().next();
        assertThat(firstBlocking2DependencyOperation.scheduledStartTime(), is(Time.fromNano(4).plus(offset)));
        assertThat(secondBlocking2DependencyOperation.scheduledStartTime(), is(Time.fromNano(5004).plus(offset)));
        assertThat(secondBlocking2DependencyOperation.scheduledStartTime().durationGreaterThan(firstBlocking2DependencyOperation.scheduledStartTime()), is(Duration.fromNano(5000)));
        assertThat(secondBlocking2DependencyOperation.dependencyTime().durationGreaterThan(firstBlocking2DependencyOperation.dependencyTime()), is(Duration.fromNano(5000)));

        Operation<?> firstBlocking2NonDependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(1).nonDependencyOperations().next();
        Operation<?> secondBlocking2NonDependencyOperation = workloadStreamsBefore.blockingStreamDefinitions().get(1).nonDependencyOperations().next();
        assertThat(firstBlocking2NonDependencyOperation.scheduledStartTime(), is(Time.fromNano(5).plus(offset)));
        assertThat(secondBlocking2NonDependencyOperation.scheduledStartTime(), is(Time.fromNano(50005).plus(offset)));
        assertThat(secondBlocking2NonDependencyOperation.scheduledStartTime().durationGreaterThan(firstBlocking2NonDependencyOperation.scheduledStartTime()), is(Duration.fromNano(50000)));
        assertThat(secondBlocking2NonDependencyOperation.dependencyTime().durationGreaterThan(firstBlocking2NonDependencyOperation.dependencyTime()), is(Duration.fromNano(50000)));
    }

    @Test
    public void shouldLimitWorkloadCorrectly() throws WorkloadException, DriverConfigurationException {
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));
        WorkloadFactory workloadFactory = new WorkloadFactory() {
            @Override
            public Workload createWorkload() throws WorkloadException {
                return new TestWorkload();
            }
        };
        ConsoleAndFileDriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(null, null, 100);
        Tuple.Tuple2<WorkloadStreams, Workload> limitedWorkloadStreamsAndWorkload = WorkloadStreams.createNewWorkloadWithLimitedWorkloadStreams(workloadFactory, configuration, gf);
        WorkloadStreams workloadStreams = limitedWorkloadStreamsAndWorkload._1();
        Workload workload = limitedWorkloadStreamsAndWorkload._2();
        assertThat(Iterators.size(workloadStreams.mergeSortedByStartTime(gf)), is(100));
        workload.cleanup();
    }

    @Test
    public void shouldLimitStreamsCorrectly() throws WorkloadException {
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));

        List<Operation<?>> stream0 = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(0), Time.fromMilli(0), "0-1"),
                new TimedNamedOperation1(Time.fromMilli(1), Time.fromMilli(0), "0-2"),
                new TimedNamedOperation1(Time.fromMilli(2), Time.fromMilli(0), "0-3"),
                new TimedNamedOperation1(Time.fromMilli(6), Time.fromMilli(0), "0-4"),
                new TimedNamedOperation1(Time.fromMilli(7), Time.fromMilli(0), "0-5")
        );

        List<Operation<?>> stream1 = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(0), Time.fromMilli(0), "1-1"),
                new TimedNamedOperation1(Time.fromMilli(3), Time.fromMilli(0), "1-2"),
                new TimedNamedOperation1(Time.fromMilli(4), Time.fromMilli(0), "1-3"),
                new TimedNamedOperation1(Time.fromMilli(9), Time.fromMilli(0), "1-4")
        );

        List<Operation<?>> stream2 = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(1), Time.fromMilli(0), "2-1"),
                new TimedNamedOperation1(Time.fromMilli(3), Time.fromMilli(0), "2-2"),
                new TimedNamedOperation1(Time.fromMilli(4), Time.fromMilli(0), "2-3"),
                new TimedNamedOperation1(Time.fromMilli(8), Time.fromMilli(0), "2-4"),
                new TimedNamedOperation1(Time.fromMilli(8), Time.fromMilli(0), "2-5"),
                new TimedNamedOperation1(Time.fromMilli(9), Time.fromMilli(0), "2-6")
        );

        List<Operation<?>> stream3 = Lists.newArrayList(
        );

        List<Operation<?>> stream4 = Lists.newArrayList(gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(Time.fromMilli(10), Duration.fromMilli(1)),
                                gf.constant(Time.fromMilli(0)),
                                gf.constant("4-x")
                        ),
                        1000000
                )
        );

        List<Iterator<Operation<?>>> streams = Lists.newArrayList(
                stream0.iterator(),
                stream1.iterator(),
                stream2.iterator(),
                stream3.iterator(),
                stream4.iterator()
        );

        long k = 10;
        long[] kForIterator = WorkloadStreams.fromAmongAllRetrieveTopK(streams, k);

        List<Operation<?>> topK = Lists.newArrayList(
                gf.mergeSortOperationsByStartTime(
                        gf.limit(
                                stream0.iterator(),
                                kForIterator[0]
                        ),
                        gf.limit(
                                stream1.iterator(),
                                kForIterator[1]
                        ),
                        gf.limit(
                                stream2.iterator(),
                                kForIterator[2]
                        ),
                        gf.limit(
                                stream3.iterator(),
                                kForIterator[3]
                        ),
                        gf.limit(
                                stream4.iterator(),
                                kForIterator[4]
                        )
                )
        );

        assertThat((long) topK.size(), is(k));
        assertThat(((TimedNamedOperation1) topK.get(0)).name(), anyOf(equalTo("0-1"), equalTo("1-1")));
        assertThat(((TimedNamedOperation1) topK.get(1)).name(), anyOf(equalTo("0-1"), equalTo("1-1")));
        assertThat(((TimedNamedOperation1) topK.get(0)).name(), not(equalTo(((TimedNamedOperation1) topK.get(1)).name())));
        assertThat(((TimedNamedOperation1) topK.get(2)).name(), anyOf(equalTo("0-2"), equalTo("2-1")));
        assertThat(((TimedNamedOperation1) topK.get(3)).name(), anyOf(equalTo("0-2"), equalTo("2-1")));
        assertThat(((TimedNamedOperation1) topK.get(2)).name(), not(equalTo(((TimedNamedOperation1) topK.get(3)).name())));
        assertThat(((TimedNamedOperation1) topK.get(4)).name(), anyOf(equalTo("0-3")));
        assertThat(((TimedNamedOperation1) topK.get(5)).name(), anyOf(equalTo("1-2"), equalTo("2-2")));
        assertThat(((TimedNamedOperation1) topK.get(6)).name(), anyOf(equalTo("1-2"), equalTo("2-2")));
        assertThat(((TimedNamedOperation1) topK.get(5)).name(), not(equalTo(((TimedNamedOperation1) topK.get(6)).name())));
        assertThat(((TimedNamedOperation1) topK.get(7)).name(), anyOf(equalTo("1-3"), equalTo("2-3")));
        assertThat(((TimedNamedOperation1) topK.get(8)).name(), anyOf(equalTo("1-3"), equalTo("2-3")));
        assertThat(((TimedNamedOperation1) topK.get(7)).name(), not(equalTo(((TimedNamedOperation1) topK.get(8)).name())));
        assertThat(((TimedNamedOperation1) topK.get(9)).name(), anyOf(equalTo("0-4")));
    }

    @Test
    public void shouldLimitStreamsCorrectlyWhenLimitIsHigherThanActualStreamsLength() throws WorkloadException {
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));

        List<Operation<?>> stream0 = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(0), Time.fromMilli(0), "0-1"),
                new TimedNamedOperation1(Time.fromMilli(1), Time.fromMilli(0), "0-2"),
                new TimedNamedOperation1(Time.fromMilli(2), Time.fromMilli(0), "0-3"),
                new TimedNamedOperation1(Time.fromMilli(6), Time.fromMilli(0), "0-4")
        );

        List<Operation<?>> stream1 = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(0), Time.fromMilli(0), "1-1"),
                new TimedNamedOperation1(Time.fromMilli(3), Time.fromMilli(0), "1-2"),
                new TimedNamedOperation1(Time.fromMilli(4), Time.fromMilli(0), "1-3")
        );

        List<Operation<?>> stream2 = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(1), Time.fromMilli(0), "2-1"),
                new TimedNamedOperation1(Time.fromMilli(3), Time.fromMilli(0), "2-2"),
                new TimedNamedOperation1(Time.fromMilli(4), Time.fromMilli(0), "2-3")
        );

        List<Operation<?>> stream3 = Lists.newArrayList(
        );

        List<Operation<?>> stream4 = Lists.newArrayList(gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(Time.fromMilli(10), Duration.fromMilli(1)),
                                gf.constant(Time.fromMilli(0)),
                                gf.constant("4-x")
                        ),
                        1000000
                )
        );

        List<Iterator<Operation<?>>> streams = Lists.newArrayList(
                stream0.iterator(),
                stream1.iterator(),
                stream2.iterator(),
                stream3.iterator(),
                stream4.iterator()
        );

        long k = 10000;
        long[] kForIterator = WorkloadStreams.fromAmongAllRetrieveTopK(streams, k);

        List<Operation<?>> topK = Lists.newArrayList(
                gf.mergeSortOperationsByStartTime(
                        gf.limit(
                                stream0.iterator(),
                                kForIterator[0]
                        ),
                        gf.limit(
                                stream1.iterator(),
                                kForIterator[1]
                        ),
                        gf.limit(
                                stream2.iterator(),
                                kForIterator[2]
                        ),
                        gf.limit(
                                stream3.iterator(),
                                kForIterator[3]
                        ),
                        gf.limit(
                                stream4.iterator(),
                                kForIterator[4]
                        )
                )
        );

        assertThat((long) topK.size(), is(k));
        assertThat(((TimedNamedOperation1) topK.get(0)).name(), anyOf(equalTo("0-1"), equalTo("1-1")));
        assertThat(((TimedNamedOperation1) topK.get(1)).name(), anyOf(equalTo("0-1"), equalTo("1-1")));
        assertThat(((TimedNamedOperation1) topK.get(0)).name(), not(equalTo(((TimedNamedOperation1) topK.get(1)).name())));
        assertThat(((TimedNamedOperation1) topK.get(2)).name(), anyOf(equalTo("0-2"), equalTo("2-1")));
        assertThat(((TimedNamedOperation1) topK.get(3)).name(), anyOf(equalTo("0-2"), equalTo("2-1")));
        assertThat(((TimedNamedOperation1) topK.get(2)).name(), not(equalTo(((TimedNamedOperation1) topK.get(3)).name())));
        assertThat(((TimedNamedOperation1) topK.get(4)).name(), anyOf(equalTo("0-3")));
        assertThat(((TimedNamedOperation1) topK.get(5)).name(), anyOf(equalTo("1-2"), equalTo("2-2")));
        assertThat(((TimedNamedOperation1) topK.get(6)).name(), anyOf(equalTo("1-2"), equalTo("2-2")));
        assertThat(((TimedNamedOperation1) topK.get(5)).name(), not(equalTo(((TimedNamedOperation1) topK.get(6)).name())));
        assertThat(((TimedNamedOperation1) topK.get(7)).name(), anyOf(equalTo("1-3"), equalTo("2-3")));
        assertThat(((TimedNamedOperation1) topK.get(8)).name(), anyOf(equalTo("1-3"), equalTo("2-3")));
        assertThat(((TimedNamedOperation1) topK.get(7)).name(), not(equalTo(((TimedNamedOperation1) topK.get(8)).name())));
        assertThat(((TimedNamedOperation1) topK.get(9)).name(), anyOf(equalTo("0-4")));
    }

    private class TestWorkload extends Workload {

        @Override
        public void onInit(Map<String, String> params) throws WorkloadException {
        }

        @Override
        protected void onCleanup() throws WorkloadException {
        }

        @Override
        protected WorkloadStreams getStreams(GeneratorFactory generators) throws WorkloadException {
            return getWorkloadStreams();
        }

        @Override
        public String serializeOperation(Operation<?> operation) throws SerializingMarshallingException {
            return null;
        }

        @Override
        public Operation<?> marshalOperation(String serializedOperation) throws SerializingMarshallingException {
            return null;
        }
    }

    private WorkloadStreams getWorkloadStreams() {
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));
        Iterator<Operation<?>> asyncDependencyStream = new TimedNamedOperation1Factory(
                gf.constantIncrementTime(Time.fromNano(0), Duration.fromNano(10)),
                gf.constantIncrementTime(Time.fromNano(0), Duration.fromNano(10)),
                gf.constant("ad")
        );
        Iterator<Operation<?>> asyncNonDependencyStream = new TimedNamedOperation1Factory(
                gf.constantIncrementTime(Time.fromNano(2), Duration.fromNano(100)),
                gf.constantIncrementTime(Time.fromNano(2), Duration.fromNano(100)),
                gf.constant("an")
        );
        Iterator<Operation<?>> blockingDependencyStream1 = new TimedNamedOperation2Factory(
                gf.constantIncrementTime(Time.fromNano(4), Duration.fromNano(1000)),
                gf.constantIncrementTime(Time.fromNano(4), Duration.fromNano(1000)),
                gf.constant("bd1")
        );
        Iterator<Operation<?>> blockingNonDependencyStream1 = new TimedNamedOperation2Factory(
                gf.constantIncrementTime(Time.fromNano(6), Duration.fromNano(10000)),
                gf.constantIncrementTime(Time.fromNano(6), Duration.fromNano(10000)),
                gf.constant("bn1")
        );
        Iterator<Operation<?>> blockingDependencyStream2 = new TimedNamedOperation3Factory(
                gf.constantIncrementTime(Time.fromNano(8), Duration.fromNano(10000)),
                gf.constantIncrementTime(Time.fromNano(8), Duration.fromNano(10000)),
                gf.constant("bd2")
        );
        Iterator<Operation<?>> blockingNonDependencyStream2 = new TimedNamedOperation3Factory(
                gf.constantIncrementTime(Time.fromNano(10), Duration.fromNano(100000)),
                gf.constantIncrementTime(Time.fromNano(10), Duration.fromNano(100000)),
                gf.constant("bn2")
        );
        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                new HashSet<Class<? extends Operation<?>>>(),
                asyncDependencyStream,
                asyncNonDependencyStream
        );
        workloadStreams.addBlockingStream(
                new HashSet<Class<? extends Operation<?>>>(),
                blockingDependencyStream1,
                blockingNonDependencyStream1
        );
        workloadStreams.addBlockingStream(
                new HashSet<Class<? extends Operation<?>>>(),
                blockingDependencyStream2,
                blockingNonDependencyStream2
        );
        return workloadStreams;
    }
}
