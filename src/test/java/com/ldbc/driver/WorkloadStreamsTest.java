package com.ldbc.driver;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.Log4jLoggingServiceFactory;
import com.ldbc.driver.control.LoggingServiceFactory;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.util.Tuple3;
import com.ldbc.driver.workloads.WorkloadFactory;
import com.ldbc.driver.workloads.dummy.NothingOperation;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1Factory;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation2;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation2Factory;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation3;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation3Factory;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class WorkloadStreamsTest
{

    @Test
    public void shouldReturnSameWorkloadStreamsAsCreatedWith()
    {
        WorkloadStreams workloadStreamsBefore = getWorkloadStreams();

        Operation firstAsyncDependencyOperation =
                workloadStreamsBefore.asynchronousStream().dependencyOperations().next();
        Operation secondAsyncDependencyOperation =
                workloadStreamsBefore.asynchronousStream().dependencyOperations().next();
        assertThat( firstAsyncDependencyOperation.scheduledStartTimeAsMilli(), is( 0l ) );
        assertThat( firstAsyncDependencyOperation.timeStamp(), is( 0l ) );
        assertThat( firstAsyncDependencyOperation.dependencyTimeStamp(), is( 0l ) );
        assertThat( secondAsyncDependencyOperation.scheduledStartTimeAsMilli(), is( 10l ) );
        assertThat( secondAsyncDependencyOperation.timeStamp(), is( 10l ) );
        assertThat( secondAsyncDependencyOperation.dependencyTimeStamp(), is( 10l ) );

        Operation firstAsyncNonDependencyOperation =
                workloadStreamsBefore.asynchronousStream().nonDependencyOperations().next();
        Operation secondAsyncNonDependencyOperation =
                workloadStreamsBefore.asynchronousStream().nonDependencyOperations().next();
        assertThat( firstAsyncNonDependencyOperation.scheduledStartTimeAsMilli(), is( 2l ) );
        assertThat( firstAsyncNonDependencyOperation.timeStamp(), is( 2l ) );
        assertThat( firstAsyncNonDependencyOperation.dependencyTimeStamp(), is( 2l ) );
        assertThat( secondAsyncNonDependencyOperation.scheduledStartTimeAsMilli(), is( 102l ) );
        assertThat( secondAsyncNonDependencyOperation.timeStamp(), is( 102l ) );
        assertThat( secondAsyncNonDependencyOperation.dependencyTimeStamp(), is( 102l ) );

        Operation firstBlocking1DependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 0 ).dependencyOperations().next();
        Operation secondBlocking1DependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 0 ).dependencyOperations().next();
        assertThat( firstBlocking1DependencyOperation.scheduledStartTimeAsMilli(), is( 4l ) );
        assertThat( firstBlocking1DependencyOperation.timeStamp(), is( 4l ) );
        assertThat( firstBlocking1DependencyOperation.dependencyTimeStamp(), is( 4l ) );
        assertThat( secondBlocking1DependencyOperation.scheduledStartTimeAsMilli(), is( 1004l ) );
        assertThat( secondBlocking1DependencyOperation.timeStamp(), is( 1004l ) );
        assertThat( secondBlocking1DependencyOperation.dependencyTimeStamp(), is( 1004l ) );

        Operation firstBlocking1NonDependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 0 ).nonDependencyOperations().next();
        Operation secondBlocking1NonDependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 0 ).nonDependencyOperations().next();
        assertThat( firstBlocking1NonDependencyOperation.scheduledStartTimeAsMilli(), is( 6l ) );
        assertThat( firstBlocking1NonDependencyOperation.timeStamp(), is( 6l ) );
        assertThat( firstBlocking1NonDependencyOperation.dependencyTimeStamp(), is( 6l ) );
        assertThat( secondBlocking1NonDependencyOperation.scheduledStartTimeAsMilli(), is( 10006l ) );
        assertThat( secondBlocking1NonDependencyOperation.timeStamp(), is( 10006l ) );
        assertThat( secondBlocking1NonDependencyOperation.dependencyTimeStamp(), is( 10006l ) );

        Operation firstBlocking2DependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 1 ).dependencyOperations().next();
        Operation secondBlocking2DependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 1 ).dependencyOperations().next();
        assertThat( firstBlocking2DependencyOperation.scheduledStartTimeAsMilli(), is( 8l ) );
        assertThat( firstBlocking2DependencyOperation.timeStamp(), is( 8l ) );
        assertThat( firstBlocking2DependencyOperation.dependencyTimeStamp(), is( 8l ) );
        assertThat( secondBlocking2DependencyOperation.scheduledStartTimeAsMilli(), is( 10008l ) );
        assertThat( secondBlocking2DependencyOperation.timeStamp(), is( 10008l ) );
        assertThat( secondBlocking2DependencyOperation.dependencyTimeStamp(), is( 10008l ) );

        Operation firstBlocking2NonDependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 1 ).nonDependencyOperations().next();
        Operation secondBlocking2NonDependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 1 ).nonDependencyOperations().next();
        assertThat( firstBlocking2NonDependencyOperation.scheduledStartTimeAsMilli(), is( 10l ) );
        assertThat( firstBlocking2NonDependencyOperation.timeStamp(), is( 10l ) );
        assertThat( firstBlocking2NonDependencyOperation.dependencyTimeStamp(), is( 10l ) );
        assertThat( secondBlocking2NonDependencyOperation.scheduledStartTimeAsMilli(), is( 100010l ) );
        assertThat( secondBlocking2NonDependencyOperation.timeStamp(), is( 100010l ) );
        assertThat( secondBlocking2NonDependencyOperation.dependencyTimeStamp(), is( 100010l ) );
    }

    @Test
    public void shouldPerformTimeOffsetCorrectly() throws WorkloadException
    {
        long offset = TimeUnit.SECONDS.toMillis( 100 );
        WorkloadStreams workloadStreamsBefore = WorkloadStreams.timeOffsetAndCompressWorkloadStreams(
                getWorkloadStreams(),
                0l + offset,
                1.0,
                new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) ) );

        Operation firstAsyncDependencyOperation =
                workloadStreamsBefore.asynchronousStream().dependencyOperations().next();
        Operation secondAsyncDependencyOperation =
                workloadStreamsBefore.asynchronousStream().dependencyOperations().next();
        assertThat( firstAsyncDependencyOperation.scheduledStartTimeAsMilli(), is( 0l + offset ) );
        assertThat( firstAsyncDependencyOperation.timeStamp(), is( 0l ) );
        assertThat( firstAsyncDependencyOperation.dependencyTimeStamp(), is( 0l ) );
        assertThat( secondAsyncDependencyOperation.scheduledStartTimeAsMilli(), is( 10l + offset ) );
        assertThat( secondAsyncDependencyOperation.timeStamp(), is( 10l ) );
        assertThat( secondAsyncDependencyOperation.dependencyTimeStamp(), is( 10l ) );

        Operation firstAsyncNonDependencyOperation =
                workloadStreamsBefore.asynchronousStream().nonDependencyOperations().next();
        Operation secondAsyncNonDependencyOperation =
                workloadStreamsBefore.asynchronousStream().nonDependencyOperations().next();
        assertThat( firstAsyncNonDependencyOperation.scheduledStartTimeAsMilli(), is( 2l + offset ) );
        assertThat( firstAsyncNonDependencyOperation.timeStamp(), is( 2l ) );
        assertThat( firstAsyncNonDependencyOperation.dependencyTimeStamp(), is( 2l ) );
        assertThat( secondAsyncNonDependencyOperation.scheduledStartTimeAsMilli(), is( 102l + offset ) );
        assertThat( secondAsyncNonDependencyOperation.timeStamp(), is( 102l ) );
        assertThat( secondAsyncNonDependencyOperation.dependencyTimeStamp(), is( 102l ) );

        Operation firstBlocking1DependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 0 ).dependencyOperations().next();
        Operation secondBlocking1DependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 0 ).dependencyOperations().next();
        assertThat( firstBlocking1DependencyOperation.scheduledStartTimeAsMilli(), is( 4l + offset ) );
        assertThat( firstBlocking1DependencyOperation.timeStamp(), is( 4l ) );
        assertThat( firstBlocking1DependencyOperation.dependencyTimeStamp(), is( 4l ) );
        assertThat( secondBlocking1DependencyOperation.scheduledStartTimeAsMilli(), is( 1004l + offset ) );
        assertThat( secondBlocking1DependencyOperation.timeStamp(), is( 1004l ) );
        assertThat( secondBlocking1DependencyOperation.dependencyTimeStamp(), is( 1004l ) );

        Operation firstBlocking1NonDependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 0 ).nonDependencyOperations().next();
        Operation secondBlocking1NonDependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 0 ).nonDependencyOperations().next();
        assertThat( firstBlocking1NonDependencyOperation.scheduledStartTimeAsMilli(), is( 6l + offset ) );
        assertThat( firstBlocking1NonDependencyOperation.timeStamp(), is( 6l ) );
        assertThat( firstBlocking1NonDependencyOperation.dependencyTimeStamp(), is( 6l ) );
        assertThat( secondBlocking1NonDependencyOperation.scheduledStartTimeAsMilli(), is( 10006l + offset ) );
        assertThat( secondBlocking1NonDependencyOperation.timeStamp(), is( 10006l ) );
        assertThat( secondBlocking1NonDependencyOperation.dependencyTimeStamp(), is( 10006l ) );

        Operation firstBlocking2DependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 1 ).dependencyOperations().next();
        Operation secondBlocking2DependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 1 ).dependencyOperations().next();
        assertThat( firstBlocking2DependencyOperation.scheduledStartTimeAsMilli(), is( 8l + offset ) );
        assertThat( firstBlocking2DependencyOperation.timeStamp(), is( 8l ) );
        assertThat( firstBlocking2DependencyOperation.dependencyTimeStamp(), is( 8l ) );
        assertThat( secondBlocking2DependencyOperation.scheduledStartTimeAsMilli(), is( 10008l + offset ) );
        assertThat( secondBlocking2DependencyOperation.timeStamp(), is( 10008l ) );
        assertThat( secondBlocking2DependencyOperation.dependencyTimeStamp(), is( 10008l ) );

        Operation firstBlocking2NonDependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 1 ).nonDependencyOperations().next();
        Operation secondBlocking2NonDependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 1 ).nonDependencyOperations().next();
        assertThat( firstBlocking2NonDependencyOperation.scheduledStartTimeAsMilli(), is( 10l + offset ) );
        assertThat( firstBlocking2NonDependencyOperation.timeStamp(), is( 10l ) );
        assertThat( firstBlocking2NonDependencyOperation.dependencyTimeStamp(), is( 10l ) );
        assertThat( secondBlocking2NonDependencyOperation.scheduledStartTimeAsMilli(), is( 100010l + offset ) );
        assertThat( secondBlocking2NonDependencyOperation.timeStamp(), is( 100010l ) );
        assertThat( secondBlocking2NonDependencyOperation.dependencyTimeStamp(), is( 100010l ) );
    }

    @Test
    public void shouldPerformTimeOffsetAndCompressionCorrectly() throws WorkloadException
    {
        long offset = TimeUnit.SECONDS.toMillis( 100 );
        WorkloadStreams workloadStreamsBefore = WorkloadStreams.timeOffsetAndCompressWorkloadStreams(
                getWorkloadStreams(),
                0l + offset,
                0.5,
                new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) ) );

        Operation firstAsyncDependencyOperation =
                workloadStreamsBefore.asynchronousStream().dependencyOperations().next();
        Operation secondAsyncDependencyOperation =
                workloadStreamsBefore.asynchronousStream().dependencyOperations().next();
        assertThat( firstAsyncDependencyOperation.scheduledStartTimeAsMilli(), is( 0l + offset ) );
        assertThat( firstAsyncDependencyOperation.timeStamp(), is( 0l ) );
        assertThat( secondAsyncDependencyOperation.scheduledStartTimeAsMilli(), is( 5l + offset ) );
        assertThat( secondAsyncDependencyOperation.timeStamp(), is( 10l ) );
        assertThat( secondAsyncDependencyOperation.scheduledStartTimeAsMilli() -
                    firstAsyncDependencyOperation.scheduledStartTimeAsMilli(), is( 5l ) );
        assertThat( secondAsyncDependencyOperation.timeStamp() - firstAsyncDependencyOperation.timeStamp(), is( 10l ) );
        assertThat( secondAsyncDependencyOperation.dependencyTimeStamp() -
                    firstAsyncDependencyOperation.dependencyTimeStamp(), is( 10l ) );

        Operation firstAsyncNonDependencyOperation =
                workloadStreamsBefore.asynchronousStream().nonDependencyOperations().next();
        Operation secondAsyncNonDependencyOperation =
                workloadStreamsBefore.asynchronousStream().nonDependencyOperations().next();
        assertThat( firstAsyncNonDependencyOperation.scheduledStartTimeAsMilli(), is( 1l + offset ) );
        assertThat( firstAsyncNonDependencyOperation.timeStamp(), is( 2l ) );
        assertThat( firstAsyncNonDependencyOperation.dependencyTimeStamp(), is( 2l ) );
        assertThat( secondAsyncNonDependencyOperation.scheduledStartTimeAsMilli(), is( 51l + offset ) );
        assertThat( secondAsyncNonDependencyOperation.timeStamp(), is( 102l ) );
        assertThat( secondAsyncNonDependencyOperation.dependencyTimeStamp(), is( 102l ) );

        Operation firstBlocking1DependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 0 ).dependencyOperations().next();
        Operation secondBlocking1DependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 0 ).dependencyOperations().next();
        assertThat( firstBlocking1DependencyOperation.scheduledStartTimeAsMilli(), is( 2l + offset ) );
        assertThat( firstBlocking1DependencyOperation.timeStamp(), is( 4l ) );
        assertThat( firstBlocking1DependencyOperation.dependencyTimeStamp(), is( 4l ) );
        assertThat( secondBlocking1DependencyOperation.scheduledStartTimeAsMilli(), is( 502l + offset ) );
        assertThat( secondBlocking1DependencyOperation.timeStamp(), is( 1004l ) );
        assertThat( secondBlocking1DependencyOperation.dependencyTimeStamp(), is( 1004l ) );

        Operation firstBlocking1NonDependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 0 ).nonDependencyOperations().next();
        Operation secondBlocking1NonDependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 0 ).nonDependencyOperations().next();
        assertThat( firstBlocking1NonDependencyOperation.scheduledStartTimeAsMilli(), is( 3l + offset ) );
        assertThat( firstBlocking1NonDependencyOperation.timeStamp(), is( 6l ) );
        assertThat( firstBlocking1NonDependencyOperation.dependencyTimeStamp(), is( 6l ) );
        assertThat( secondBlocking1NonDependencyOperation.scheduledStartTimeAsMilli(), is( 5003l + offset ) );
        assertThat( secondBlocking1NonDependencyOperation.timeStamp(), is( 10006l ) );
        assertThat( secondBlocking1NonDependencyOperation.dependencyTimeStamp(), is( 10006l ) );

        Operation firstBlocking2DependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 1 ).dependencyOperations().next();
        Operation secondBlocking2DependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 1 ).dependencyOperations().next();
        assertThat( firstBlocking2DependencyOperation.scheduledStartTimeAsMilli(), is( 4l + offset ) );
        assertThat( firstBlocking2DependencyOperation.timeStamp(), is( 8l ) );
        assertThat( firstBlocking2DependencyOperation.dependencyTimeStamp(), is( 8l ) );
        assertThat( secondBlocking2DependencyOperation.scheduledStartTimeAsMilli(), is( 5004l + offset ) );
        assertThat( secondBlocking2DependencyOperation.timeStamp(), is( 10008l ) );
        assertThat( secondBlocking2DependencyOperation.dependencyTimeStamp(), is( 10008l ) );

        Operation firstBlocking2NonDependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 1 ).nonDependencyOperations().next();
        Operation secondBlocking2NonDependencyOperation =
                workloadStreamsBefore.blockingStreamDefinitions().get( 1 ).nonDependencyOperations().next();
        assertThat( firstBlocking2NonDependencyOperation.scheduledStartTimeAsMilli(), is( 5l + offset ) );
        assertThat( firstBlocking2NonDependencyOperation.timeStamp(), is( 10l ) );
        assertThat( firstBlocking2NonDependencyOperation.dependencyTimeStamp(), is( 10l ) );
        assertThat( secondBlocking2NonDependencyOperation.scheduledStartTimeAsMilli(), is( 50005l + offset ) );
        assertThat( secondBlocking2NonDependencyOperation.timeStamp(), is( 100010l ) );
        assertThat( secondBlocking2NonDependencyOperation.dependencyTimeStamp(), is( 100010l ) );
    }

    @Test
    public void shouldLimitWorkloadCorrectly() throws WorkloadException, DriverConfigurationException, IOException
    {
        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) );
        WorkloadFactory workloadFactory = new WorkloadFactory()
        {
            @Override
            public Workload createWorkload() throws WorkloadException
            {
                return new TestWorkload();
            }
        };
        ConsoleAndFileDriverConfiguration configuration =
                ConsoleAndFileDriverConfiguration.fromDefaults( null, null, 100 );
        boolean returnStreamsWithDbConnector = false;
        LoggingServiceFactory loggingServiceFactory = new Log4jLoggingServiceFactory( false );
        Tuple3<WorkloadStreams,Workload,Long> limitedWorkloadStreamsAndWorkload =
                WorkloadStreams.createNewWorkloadWithOffsetAndLimitedWorkloadStreams(
                        workloadFactory,
                        configuration,
                        gf,
                        returnStreamsWithDbConnector,
                        0,
                        configuration.operationCount(),
                        loggingServiceFactory
                );
        WorkloadStreams workloadStreams = limitedWorkloadStreamsAndWorkload._1();
        Workload workload = limitedWorkloadStreamsAndWorkload._2();
        assertThat( Iterators
                        .size( WorkloadStreams
                                .mergeSortedByStartTimeExcludingChildOperationGenerators( gf, workloadStreams ) ),
                is( 100 ) );
        workload.close();
    }

    @Test
    public void shouldLimitWorkloadCorrectly_WITH_OFFSET()
            throws WorkloadException, DriverConfigurationException, IOException
    {
        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) );
        WorkloadFactory workloadFactory = new WorkloadFactory()
        {
            @Override
            public Workload createWorkload() throws WorkloadException
            {
                return new TestWorkload();
            }
        };
        ConsoleAndFileDriverConfiguration configuration =
                ConsoleAndFileDriverConfiguration.fromDefaults( null, null, 100 );
        configuration = (ConsoleAndFileDriverConfiguration) configuration
                .applyArg( ConsoleAndFileDriverConfiguration.WARMUP_COUNT_ARG, Long.toString( 10 ) );
        boolean returnStreamsWithDbConnector = false;
        LoggingServiceFactory loggingServiceFactory = new Log4jLoggingServiceFactory( false );
        Tuple3<WorkloadStreams,Workload,Long> limitedWorkloadStreamsAndWorkload =
                WorkloadStreams.createNewWorkloadWithOffsetAndLimitedWorkloadStreams(
                        workloadFactory,
                        configuration,
                        gf,
                        returnStreamsWithDbConnector,
                        configuration.warmupCount(),
                        configuration.operationCount(),
                        loggingServiceFactory
                );
        WorkloadStreams workloadStreams = limitedWorkloadStreamsAndWorkload._1();
        Workload workload = limitedWorkloadStreamsAndWorkload._2();
        assertThat( Iterators
                        .size( WorkloadStreams
                                .mergeSortedByStartTimeExcludingChildOperationGenerators( gf, workloadStreams ) ),
                is( 100 ) );
        workload.close();
    }

    @Test
    public void shouldLimitStreamsCorrectly() throws WorkloadException
    {
        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) );

        List<Operation> stream0 = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 0l, 0l, 0l, "0-1" ),
                new TimedNamedOperation1( 1l, 1l, 0l, "0-2" ),
                new TimedNamedOperation1( 2l, 2l, 0l, "0-3" ),
                new TimedNamedOperation1( 6l, 6l, 0l, "0-4" ),
                new TimedNamedOperation1( 7l, 7l, 0l, "0-5" )
        );

        List<Operation> stream1 = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 0l, 0l, 0l, "1-1" ),
                new TimedNamedOperation1( 3l, 3l, 0l, "1-2" ),
                new TimedNamedOperation1( 4l, 4l, 0l, "1-3" ),
                new TimedNamedOperation1( 9l, 9l, 0l, "1-4" )
        );

        List<Operation> stream2 = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 1l, 1l, 0l, "2-1" ),
                new TimedNamedOperation1( 3l, 3l, 0l, "2-2" ),
                new TimedNamedOperation1( 4l, 4l, 0l, "2-3" ),
                new TimedNamedOperation1( 8l, 8l, 0l, "2-4" ),
                new TimedNamedOperation1( 8l, 8l, 0l, "2-5" ),
                new TimedNamedOperation1( 9l, 9l, 0l, "2-6" )
        );

        List<Operation> stream3 = Lists.newArrayList(
        );

        List<Operation> stream4 = Lists.newArrayList( gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.incrementing( 10l, 1l ),
                                gf.constant( 0l ),
                                gf.constant( "4-x" )
                        ),
                        1000000
                )
        );

        List<Iterator<Operation>> streams = Lists.newArrayList(
                stream0.iterator(),
                stream1.iterator(),
                stream2.iterator(),
                stream3.iterator(),
                stream4.iterator()
        );

        List<ChildOperationGenerator> childOperationGenerators = Lists.newArrayList(
                null,
                null,
                null,
                null,
                null
        );

        long offset = 0;
        long count = 10;
        LoggingServiceFactory loggingServiceFactory = new Log4jLoggingServiceFactory( false );
        Tuple3<long[],long[],Long> kForIteratorAndMinimums =
                WorkloadStreams.fromAmongAllRetrieveTopCountFromOffset(
                        streams,
                        offset,
                        count,
                        childOperationGenerators,
                        loggingServiceFactory
                );
        long[] startForIterator = kForIteratorAndMinimums._1();
        long[] countForIterator = kForIteratorAndMinimums._2();
        long minimumTimeStamp = kForIteratorAndMinimums._3();

        List<Operation> topK = Lists.newArrayList(
                gf.mergeSortOperationsByTimeStamp(
                        gf.limit(
                                stream0.iterator(),
                                countForIterator[0]
                        ),
                        gf.limit(
                                stream1.iterator(),
                                countForIterator[1]
                        ),
                        gf.limit(
                                stream2.iterator(),
                                countForIterator[2]
                        ),
                        gf.limit(
                                stream3.iterator(),
                                countForIterator[3]
                        ),
                        gf.limit(
                                stream4.iterator(),
                                countForIterator[4]
                        )
                )
        );

        assertThat( (long) topK.size(), is( count ) );
        assertThat( minimumTimeStamp, is( 0l ) );
        assertThat( ((TimedNamedOperation1) topK.get( 0 )).name(), anyOf( equalTo( "0-1" ), equalTo( "1-1" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 1 )).name(), anyOf( equalTo( "0-1" ), equalTo( "1-1" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 0 )).name(),
                not( equalTo( ((TimedNamedOperation1) topK.get( 1 )).name() ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 2 )).name(), anyOf( equalTo( "0-2" ), equalTo( "2-1" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 3 )).name(), anyOf( equalTo( "0-2" ), equalTo( "2-1" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 2 )).name(),
                not( equalTo( ((TimedNamedOperation1) topK.get( 3 )).name() ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 4 )).name(), anyOf( equalTo( "0-3" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 5 )).name(), anyOf( equalTo( "1-2" ), equalTo( "2-2" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 6 )).name(), anyOf( equalTo( "1-2" ), equalTo( "2-2" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 5 )).name(),
                not( equalTo( ((TimedNamedOperation1) topK.get( 6 )).name() ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 7 )).name(), anyOf( equalTo( "1-3" ), equalTo( "2-3" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 8 )).name(), anyOf( equalTo( "1-3" ), equalTo( "2-3" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 7 )).name(),
                not( equalTo( ((TimedNamedOperation1) topK.get( 8 )).name() ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 9 )).name(), anyOf( equalTo( "0-4" ) ) );
    }

    @Test
    public void shouldLimitStreamsCorrectlyWhenLimitIsHigherThanActualStreamsLength() throws WorkloadException
    {
        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) );

        List<Operation> stream0 = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 0l, 0l, 0l, "0-1" ),
                new TimedNamedOperation1( 1l, 1l, 0l, "0-2" ),
                new TimedNamedOperation1( 2l, 2l, 0l, "0-3" ),
                new TimedNamedOperation1( 6l, 6l, 0l, "0-4" )
        );

        List<Operation> stream1 = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 0l, 0l, 0l, "1-1" ),
                new TimedNamedOperation1( 3l, 3l, 0l, "1-2" ),
                new TimedNamedOperation1( 4l, 4l, 0l, "1-3" )
        );

        List<Operation> stream2 = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 1l, 1l, 0l, "2-1" ),
                new TimedNamedOperation1( 3l, 3l, 0l, "2-2" ),
                new TimedNamedOperation1( 4l, 4l, 0l, "2-3" )
        );

        List<Operation> stream3 = Lists.newArrayList(
        );

        List<Operation> stream4 = Lists.newArrayList( gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.incrementing( 10l, 1l ),
                                gf.constant( 0l ),
                                gf.constant( "4-x" )
                        ),
                        1000000
                )
        );

        List<Iterator<Operation>> streams = Lists.newArrayList(
                stream0.iterator(),
                stream1.iterator(),
                stream2.iterator(),
                stream3.iterator(),
                stream4.iterator()
        );

        List<ChildOperationGenerator> childOperationGenerators = Lists.newArrayList(
                null,
                null,
                null,
                null,
                null
        );

        long offset = 0;
        long count = 10000;
        LoggingServiceFactory loggingServiceFactory = new Log4jLoggingServiceFactory( false );
        Tuple3<long[],long[],Long> kForIteratorAndMinimums =
                WorkloadStreams.fromAmongAllRetrieveTopCountFromOffset(
                        streams,
                        offset,
                        count,
                        childOperationGenerators,
                        loggingServiceFactory
                );
        long[] startForIterator = kForIteratorAndMinimums._1();
        long[] countForIterator = kForIteratorAndMinimums._2();
        long minimumTimeStamp = kForIteratorAndMinimums._3();

        List<Operation> topK = Lists.newArrayList(
                gf.mergeSortOperationsByTimeStamp(
                        gf.limit(
                                stream0.iterator(),
                                countForIterator[0]
                        ),
                        gf.limit(
                                stream1.iterator(),
                                countForIterator[1]
                        ),
                        gf.limit(
                                stream2.iterator(),
                                countForIterator[2]
                        ),
                        gf.limit(
                                stream3.iterator(),
                                countForIterator[3]
                        ),
                        gf.limit(
                                stream4.iterator(),
                                countForIterator[4]
                        )
                )
        );

        assertThat( (long) topK.size(), is( count ) );
        assertThat( minimumTimeStamp, is( 0l ) );
        assertThat( ((TimedNamedOperation1) topK.get( 0 )).name(), anyOf( equalTo( "0-1" ), equalTo( "1-1" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 1 )).name(), anyOf( equalTo( "0-1" ), equalTo( "1-1" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 0 )).name(),
                not( equalTo( ((TimedNamedOperation1) topK.get( 1 )).name() ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 2 )).name(), anyOf( equalTo( "0-2" ), equalTo( "2-1" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 3 )).name(), anyOf( equalTo( "0-2" ), equalTo( "2-1" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 2 )).name(),
                not( equalTo( ((TimedNamedOperation1) topK.get( 3 )).name() ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 4 )).name(), anyOf( equalTo( "0-3" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 5 )).name(), anyOf( equalTo( "1-2" ), equalTo( "2-2" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 6 )).name(), anyOf( equalTo( "1-2" ), equalTo( "2-2" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 5 )).name(),
                not( equalTo( ((TimedNamedOperation1) topK.get( 6 )).name() ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 7 )).name(), anyOf( equalTo( "1-3" ), equalTo( "2-3" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 8 )).name(), anyOf( equalTo( "1-3" ), equalTo( "2-3" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 7 )).name(),
                not( equalTo( ((TimedNamedOperation1) topK.get( 8 )).name() ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 9 )).name(), anyOf( equalTo( "0-4" ) ) );
    }

    @Test
    public void shouldStartAtOffsetAndLimitStreamsCorrectlyWhenLimitIsLowerThanStreamsLength() throws WorkloadException
    {
        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) );

        List<Operation> stream0 = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 0l, 0l, 0l, "0-1--0" ),
                new TimedNamedOperation1( 1l, 1l, 0l, "0-2--1" ),
                new TimedNamedOperation1( 2l, 2l, 0l, "0-3--2" ),
                new TimedNamedOperation1( 6l, 6l, 0l, "0-4--6" ),
                new TimedNamedOperation1( 7l, 7l, 0l, "0-5--7" )
        );

        List<Operation> stream1 = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 0l, 0l, 0l, "1-1--0" ),
                new TimedNamedOperation1( 3l, 3l, 0l, "1-2--3" ),
                new TimedNamedOperation1( 4l, 4l, 0l, "1-3--4" ),
                new TimedNamedOperation1( 9l, 9l, 0l, "1-4--9" )
        );

        List<Operation> stream2 = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 1l, 1l, 0l, "2-1--1" ),
                new TimedNamedOperation1( 3l, 3l, 0l, "2-2--3" ),
                new TimedNamedOperation1( 4l, 4l, 0l, "2-3--4" ),
                new TimedNamedOperation1( 8l, 8l, 0l, "2-4--8" ),
                new TimedNamedOperation1( 8l, 8l, 0l, "2-5--8" ),
                new TimedNamedOperation1( 9l, 9l, 0l, "2-6--9" )
        );

        List<Operation> stream3 = Lists.newArrayList(
        );

        List<Operation> stream4 = Lists.newArrayList(
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.incrementing( 10l, 1l ),
                                gf.constant( 0l ),
                                gf.constant( "4-x--y" )
                        ),
                        1000000
                )
        );

        List<Iterator<Operation>> streams = Lists.newArrayList(
                stream0.iterator(),
                stream1.iterator(),
                stream2.iterator(),
                stream3.iterator(),
                stream4.iterator()
        );

        List<ChildOperationGenerator> childOperationGenerators = Lists.newArrayList(
                null,
                null,
                null,
                null,
                null
        );

        long offset = 5;
        long count = 6;
        LoggingServiceFactory loggingServiceFactory = new Log4jLoggingServiceFactory( false );
        Tuple3<long[],long[],Long> kForIteratorAndMinimums =
                WorkloadStreams.fromAmongAllRetrieveTopCountFromOffset(
                        streams,
                        offset,
                        count,
                        childOperationGenerators,
                        loggingServiceFactory
                );
        long[] startForIterator = kForIteratorAndMinimums._1();
        long[] countForIterator = kForIteratorAndMinimums._2();
        long minimumTimeStamp = kForIteratorAndMinimums._3();

        List<Iterator<Operation>> offsetStreams = Lists.newArrayList(
                stream0.iterator(),
                stream1.iterator(),
                stream2.iterator(),
                stream3.iterator(),
                stream4.iterator()
        );

        for ( int i = 0; i < offsetStreams.size(); i++ )
        {
            Iterator<Operation> offsetStream = offsetStreams.get( i );
            gf.consume( offsetStream, startForIterator[i] );
        }

        List<Operation> topK = Lists.newArrayList(
                gf.mergeSortOperationsByTimeStamp(
                        gf.limit(
                                offsetStreams.get( 0 ),
                                countForIterator[0]
                        ),
                        gf.limit(
                                offsetStreams.get( 1 ),
                                countForIterator[1]
                        ),
                        gf.limit(
                                offsetStreams.get( 2 ),
                                countForIterator[2]
                        ),
                        gf.limit(
                                offsetStreams.get( 3 ),
                                countForIterator[3]
                        ),
                        gf.limit(
                                offsetStreams.get( 4 ),
                                countForIterator[4]
                        )
                )
        );

        /*
        offset = 5
        count = 6

        TimeStamp       Operation
        0               0-1--0, 1-1--0
        1               0-2--1, 2-1--1
        2               0-3--2
        ----- 5 -----
        3               1-2--3, 2-2--3
        4               1-3--4, 2-3--4
        6               0-4--6
        ----- 10 ----
        7               0-5--7
        8               2-4--8, 2-5--8
        9               1-4--9, 2-6--9
        ----- 15 ----
        10              4-x--y
        11              4-x--y
        ...
        1000000         4-x--y
         */

        assertThat( (long) topK.size(), is( count ) );
        assertThat( minimumTimeStamp, is( 3l ) );
        assertThat( startForIterator.length, is( 5 ) );
        assertThat( countForIterator.length, is( 5 ) );

        assertThat( ((TimedNamedOperation1) topK.get( 0 )).name(), anyOf( equalTo( "1-2--3" ), equalTo( "2-2--3" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 1 )).name(), anyOf( equalTo( "1-2--3" ), equalTo( "2-2--3" ) ) );
        // does not return the same operation twice, when their time stamps are equal
        assertThat( ((TimedNamedOperation1) topK.get( 0 )).name(),
                not( equalTo( ((TimedNamedOperation1) topK.get( 1 )).name() ) ) );

        assertThat( ((TimedNamedOperation1) topK.get( 2 )).name(), anyOf( equalTo( "1-3--4" ), equalTo( "2-3--4" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 3 )).name(), anyOf( equalTo( "1-3--4" ), equalTo( "2-3--4" ) ) );
        // does not return the same operation twice, when their time stamps are equal
        assertThat( ((TimedNamedOperation1) topK.get( 2 )).name(),
                not( equalTo( ((TimedNamedOperation1) topK.get( 3 )).name() ) ) );

        assertThat( ((TimedNamedOperation1) topK.get( 4 )).name(), anyOf( equalTo( "0-4--6" ) ) );

        assertThat( ((TimedNamedOperation1) topK.get( 5 )).name(), anyOf( equalTo( "0-5--7" ) ) );
    }

    @Test
    public void shouldStartAtOffsetAndLimitStreamsCorrectlyWhenLimitIsHigherThanStreamsLength() throws WorkloadException
    {
        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) );

        List<Operation> stream0 = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 0l, 0l, 0l, "0-1--0" ),
                new TimedNamedOperation1( 1l, 1l, 0l, "0-2--1" ),
                new TimedNamedOperation1( 2l, 2l, 0l, "0-3--2" ),
                new TimedNamedOperation1( 6l, 6l, 0l, "0-4--6" ),
                new TimedNamedOperation1( 7l, 7l, 0l, "0-5--7" )
        );

        List<Operation> stream1 = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 0l, 0l, 0l, "1-1--0" ),
                new TimedNamedOperation1( 3l, 3l, 0l, "1-2--3" ),
                new TimedNamedOperation1( 4l, 4l, 0l, "1-3--4" ),
                new TimedNamedOperation1( 9l, 9l, 0l, "1-4--9" )
        );

        List<Operation> stream2 = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 1l, 1l, 0l, "2-1--1" ),
                new TimedNamedOperation1( 3l, 3l, 0l, "2-2--3" ),
                new TimedNamedOperation1( 4l, 4l, 0l, "2-3--4" ),
                new TimedNamedOperation1( 8l, 8l, 0l, "2-4--8" ),
                new TimedNamedOperation1( 8l, 8l, 0l, "2-5--8" ),
                new TimedNamedOperation1( 9l, 9l, 0l, "2-6--9" )
        );

        List<Operation> stream3 = Lists.newArrayList(
        );

        List<Iterator<Operation>> streams = Lists.newArrayList(
                stream0.iterator(),
                stream1.iterator(),
                stream2.iterator(),
                stream3.iterator()
        );

        List<ChildOperationGenerator> childOperationGenerators = Lists.newArrayList(
                null,
                null,
                null,
                null,
                null
        );

        long offset = 3;
        long count = 100;
        LoggingServiceFactory loggingServiceFactory = new Log4jLoggingServiceFactory( false );
        Tuple3<long[],long[],Long> kForIteratorAndMinimums =
                WorkloadStreams.fromAmongAllRetrieveTopCountFromOffset(
                        streams,
                        offset,
                        count,
                        childOperationGenerators,
                        loggingServiceFactory
                );
        long[] startForIterator = kForIteratorAndMinimums._1();
        long[] countForIterator = kForIteratorAndMinimums._2();
        long minimumTimeStamp = kForIteratorAndMinimums._3();

        List<Iterator<Operation>> offsetStreams = Lists.newArrayList(
                stream0.iterator(),
                stream1.iterator(),
                stream2.iterator(),
                stream3.iterator()
        );

        for ( int i = 0; i < offsetStreams.size(); i++ )
        {
            Iterator<Operation> offsetStream = offsetStreams.get( i );
            gf.consume( offsetStream, startForIterator[i] );
        }

        List<Operation> topK = Lists.newArrayList(
                gf.mergeSortOperationsByTimeStamp(
                        gf.limit(
                                offsetStreams.get( 0 ),
                                countForIterator[0]
                        ),
                        gf.limit(
                                offsetStreams.get( 1 ),
                                countForIterator[1]
                        ),
                        gf.limit(
                                offsetStreams.get( 2 ),
                                countForIterator[2]
                        ),
                        gf.limit(
                                offsetStreams.get( 3 ),
                                countForIterator[3]
                        )
                )
        );

        /*
        offset = 3
        count = 100

        TimeStamp       Operation
        0               0-1--0, 1-1--0
        ----- 2 -----
        1               0-2--1, 2-1--1
        ----- 4 -----
        2               0-3--2
        3               1-2--3, 2-2--3
        4               1-3--4, 2-3--4
        6               0-4--6
        7               0-5--7
        8               2-4--8, 2-5--8
        9               1-4--9, 2-6--9
         */

        assertThat( (long) topK.size(), is( 12l ) );
        assertThat( minimumTimeStamp, is( 1l ) );
        assertThat( startForIterator.length, is( 4 ) );
        assertThat( countForIterator.length, is( 4 ) );

        assertThat( ((TimedNamedOperation1) topK.get( 0 )).name(), anyOf( equalTo( "0-2--1" ), equalTo( "2-1--1" ) ) );

        assertThat( ((TimedNamedOperation1) topK.get( 1 )).name(), anyOf( equalTo( "0-3--2" ) ) );

        assertThat( ((TimedNamedOperation1) topK.get( 2 )).name(), anyOf( equalTo( "1-2--3" ), equalTo( "2-2--3" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 3 )).name(), anyOf( equalTo( "1-2--3" ), equalTo( "2-2--3" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 2 )).name(),
                not( equalTo( ((TimedNamedOperation1) topK.get( 3 )).name() ) ) );

        assertThat( ((TimedNamedOperation1) topK.get( 4 )).name(), anyOf( equalTo( "1-3--4" ), equalTo( "2-3--4" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 5 )).name(), anyOf( equalTo( "1-3--4" ), equalTo( "2-3--4" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 4 )).name(),
                not( equalTo( ((TimedNamedOperation1) topK.get( 5 )).name() ) ) );

        assertThat( ((TimedNamedOperation1) topK.get( 6 )).name(), anyOf( equalTo( "0-4--6" ) ) );

        assertThat( ((TimedNamedOperation1) topK.get( 7 )).name(), anyOf( equalTo( "0-5--7" ) ) );

        assertThat( ((TimedNamedOperation1) topK.get( 8 )).name(), anyOf( equalTo( "2-4--8" ), equalTo( "2-5--8" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 9 )).name(), anyOf( equalTo( "2-4--8" ), equalTo( "2-5--8" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 8 )).name(),
                not( equalTo( ((TimedNamedOperation1) topK.get( 9 )).name() ) ) );


        assertThat( ((TimedNamedOperation1) topK.get( 10 )).name(), anyOf( equalTo( "1-4--9" ), equalTo( "2-6--9" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 11 )).name(), anyOf( equalTo( "1-4--9" ), equalTo( "2-6--9" ) ) );
        assertThat( ((TimedNamedOperation1) topK.get( 10 )).name(),
                not( equalTo( ((TimedNamedOperation1) topK.get( 11 )).name() ) ) );
    }

    private class TestWorkload extends Workload
    {

        @Override
        public Map<Integer,Class<? extends Operation>> operationTypeToClassMapping()
        {
            Map<Integer,Class<? extends Operation>> operationTypeToClassMapping = new HashMap<>();
            operationTypeToClassMapping.put( NothingOperation.TYPE, NothingOperation.class );
            operationTypeToClassMapping.put( TimedNamedOperation1.TYPE, TimedNamedOperation1.class );
            operationTypeToClassMapping.put( TimedNamedOperation2.TYPE, TimedNamedOperation2.class );
            operationTypeToClassMapping.put( TimedNamedOperation3.TYPE, TimedNamedOperation3.class );
            return operationTypeToClassMapping;
        }

        @Override
        public void onInit( Map<String,String> params ) throws WorkloadException
        {
        }

        @Override
        protected void onClose() throws IOException
        {
        }

        @Override
        protected WorkloadStreams getStreams( GeneratorFactory generators, boolean hasDbConnected )
                throws WorkloadException
        {
            return getWorkloadStreams();
        }

        @Override
        public String serializeOperation( Operation operation ) throws SerializingMarshallingException
        {
            return null;
        }

        @Override
        public Operation marshalOperation( String serializedOperation ) throws SerializingMarshallingException
        {
            return null;
        }

        @Override
        public boolean resultsEqual( Operation operation, Object result1, Object result2 ) throws WorkloadException
        {
            if ( null == result1 || null == result2 )
            {
                return false;
            }
            else
            {
                return result1.equals( result2 );
            }
        }
    }

    private WorkloadStreams getWorkloadStreams()
    {
        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) );
        Iterator<Operation> asyncDependencyStream = new TimedNamedOperation1Factory(
                gf.incrementing( 0l, 10l ),
                gf.incrementing( 0l, 10l ),
                gf.constant( "ad" )
        );
        Iterator<Operation> asyncNonDependencyStream = new TimedNamedOperation1Factory(
                gf.incrementing( 2l, 100l ),
                gf.incrementing( 2l, 100l ),
                gf.constant( "an" )
        );
        Iterator<Operation> blockingDependencyStream1 = new TimedNamedOperation2Factory(
                gf.incrementing( 4l, 1000l ),
                gf.incrementing( 4l, 1000l ),
                gf.constant( "bd1" )
        );
        Iterator<Operation> blockingNonDependencyStream1 = new TimedNamedOperation2Factory(
                gf.incrementing( 6l, 10000l ),
                gf.incrementing( 6l, 10000l ),
                gf.constant( "bn1" )
        );
        Iterator<Operation> blockingDependencyStream2 = new TimedNamedOperation3Factory(
                gf.incrementing( 8l, 10000l ),
                gf.incrementing( 8l, 10000l ),
                gf.constant( "bd2" )
        );
        Iterator<Operation> blockingNonDependencyStream2 = new TimedNamedOperation3Factory(
                gf.incrementing( 10l, 100000l ),
                gf.incrementing( 10l, 100000l ),
                gf.constant( "bn2" )
        );
        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                new HashSet<Class<? extends Operation>>(),
                Sets.<Class<? extends Operation>>newHashSet( TimedNamedOperation1.class ),
                asyncDependencyStream,
                asyncNonDependencyStream,
                null
        );
        workloadStreams.addBlockingStream(
                new HashSet<Class<? extends Operation>>(),
                Sets.<Class<? extends Operation>>newHashSet( TimedNamedOperation2.class ),
                blockingDependencyStream1,
                blockingNonDependencyStream1,
                null
        );
        workloadStreams.addBlockingStream(
                new HashSet<Class<? extends Operation>>(),
                Sets.<Class<? extends Operation>>newHashSet( TimedNamedOperation3.class ),
                blockingDependencyStream2,
                blockingNonDependencyStream2,
                null
        );
        return workloadStreams;
    }
}
