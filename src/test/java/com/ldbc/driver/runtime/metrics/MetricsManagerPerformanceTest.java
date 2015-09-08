package com.ldbc.driver.runtime.metrics;

import com.google.common.collect.Lists;
import com.ldbc.driver.control.Log4jLoggingServiceFactory;
import com.ldbc.driver.control.LoggingServiceFactory;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.DefaultQueues;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;
import org.HdrHistogram.Histogram;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.concurrent.locks.LockSupport;

import static java.lang.String.format;

@Ignore
public class MetricsManagerPerformanceTest
{
    final static LdbcQuery1 LDBC_QUERY_1 = new LdbcQuery1( 1, null, 2 );
    final static LdbcQuery2 LDBC_QUERY_2 = new LdbcQuery2( 1, null, 2 );
    final static LdbcQuery3 LDBC_QUERY_3 = new LdbcQuery3( 1, null, null, null, 2, 3 );
    final static LdbcQuery4 LDBC_QUERY_4 = new LdbcQuery4( 1, null, 2, 3 );
    final static LdbcQuery5 LDBC_QUERY_5 = new LdbcQuery5( 1, null, 2 );
    final static LdbcQuery6 LDBC_QUERY_6 = new LdbcQuery6( 1, null, 2 );
    final static LdbcQuery7 LDBC_QUERY_7 = new LdbcQuery7( 1, 2 );
    final static LdbcQuery8 LDBC_QUERY_8 = new LdbcQuery8( 1, 2 );
    final static LdbcQuery9 LDBC_QUERY_9 = new LdbcQuery9( 1, null, 2 );
    final static LdbcQuery10 LDBC_QUERY_10 = new LdbcQuery10( 1, 2, 3 );
    final static LdbcQuery11 LDBC_QUERY_11 = new LdbcQuery11( 1, null, 2, 3 );
    final static LdbcQuery12 LDBC_QUERY_12 = new LdbcQuery12( 1, null, 2 );
    final static LdbcQuery13 LDBC_QUERY_13 = new LdbcQuery13( 1, 2 );
    final static LdbcQuery14 LDBC_QUERY_14 = new LdbcQuery14( 1, 2 );
    final static LdbcShortQuery1PersonProfile LDBC_SHORT_QUERY_1_PERSON_PROFILE = new LdbcShortQuery1PersonProfile( 1 );
    final static LdbcShortQuery2PersonPosts LDBC_SHORT_QUERY_2_PERSON_POSTS = new LdbcShortQuery2PersonPosts( 1, 2 );
    final static LdbcShortQuery3PersonFriends LDBC_SHORT_QUERY_3_PERSON_FRIENDS = new LdbcShortQuery3PersonFriends( 1 );
    final static LdbcShortQuery4MessageContent LDBC_SHORT_QUERY_4_MESSAGE_CONTENT =
            new LdbcShortQuery4MessageContent( 1 );
    final static LdbcShortQuery5MessageCreator LDBC_SHORT_QUERY_5_MESSAGE_CREATOR =
            new LdbcShortQuery5MessageCreator( 1 );
    final static LdbcShortQuery6MessageForum LDBC_SHORT_QUERY_6_MESSAGE_FORUM = new LdbcShortQuery6MessageForum( 1 );
    final static LdbcShortQuery7MessageReplies LDBC_SHORT_QUERY_7_MESSAGE_REPLIES =
            new LdbcShortQuery7MessageReplies( 1 );
    final static LdbcUpdate1AddPerson LDBC_UPDATE_1_ADD_PERSON =
            new LdbcUpdate1AddPerson( 1, null, null, null, null, null, null, null, 2, null, null, null, null, null );
    final static LdbcUpdate2AddPostLike LDBC_UPDATE_2_ADD_POST_LIKE = new LdbcUpdate2AddPostLike( 1, 2, null );
    final static LdbcUpdate3AddCommentLike LDBC_UPDATE_3_ADD_COMMENT_LIKE = new LdbcUpdate3AddCommentLike( 1, 2, null );
    final static LdbcUpdate4AddForum LDBC_UPDATE_4_ADD_FORUM = new LdbcUpdate4AddForum( 1, null, null, 2, null );
    final static LdbcUpdate5AddForumMembership LDBC_UPDATE_5_ADD_FORUM_MEMBERSHIP =
            new LdbcUpdate5AddForumMembership( 1, 2, null );
    final static LdbcUpdate6AddPost LDBC_UPDATE_6_ADD_POST =
            new LdbcUpdate6AddPost( 1, null, null, null, null, null, null, 2, 3, 4, 5, null );
    final static LdbcUpdate7AddComment LDBC_UPDATE_7_ADD_COMMENT =
            new LdbcUpdate7AddComment( 1, null, null, null, null, 2, 3, 4, 5, 6, null );
    final static LdbcUpdate8AddFriendship LDBC_UPDATE_8_ADD_FRIENDSHIP = new LdbcUpdate8AddFriendship( 1, 2, null );
    final static LoggingServiceFactory LOGGING_SERVICE_FACTORY = new Log4jLoggingServiceFactory( false );

    static
    {
        LDBC_QUERY_1.setScheduledStartTimeAsMilli( 1 );
        LDBC_QUERY_2.setScheduledStartTimeAsMilli( 1 );
        LDBC_QUERY_3.setScheduledStartTimeAsMilli( 1 );
        LDBC_QUERY_4.setScheduledStartTimeAsMilli( 1 );
        LDBC_QUERY_5.setScheduledStartTimeAsMilli( 1 );
        LDBC_QUERY_6.setScheduledStartTimeAsMilli( 1 );
        LDBC_QUERY_7.setScheduledStartTimeAsMilli( 1 );
        LDBC_QUERY_8.setScheduledStartTimeAsMilli( 1 );
        LDBC_QUERY_9.setScheduledStartTimeAsMilli( 1 );
        LDBC_QUERY_10.setScheduledStartTimeAsMilli( 1 );
        LDBC_QUERY_11.setScheduledStartTimeAsMilli( 1 );
        LDBC_QUERY_12.setScheduledStartTimeAsMilli( 1 );
        LDBC_QUERY_13.setScheduledStartTimeAsMilli( 1 );
        LDBC_QUERY_14.setScheduledStartTimeAsMilli( 1 );
        LDBC_SHORT_QUERY_1_PERSON_PROFILE.setScheduledStartTimeAsMilli( 1 );
        LDBC_SHORT_QUERY_2_PERSON_POSTS.setScheduledStartTimeAsMilli( 1 );
        LDBC_SHORT_QUERY_3_PERSON_FRIENDS.setScheduledStartTimeAsMilli( 1 );
        LDBC_SHORT_QUERY_4_MESSAGE_CONTENT.setScheduledStartTimeAsMilli( 1 );
        LDBC_SHORT_QUERY_5_MESSAGE_CREATOR.setScheduledStartTimeAsMilli( 1 );
        LDBC_SHORT_QUERY_6_MESSAGE_FORUM.setScheduledStartTimeAsMilli( 1 );
        LDBC_SHORT_QUERY_7_MESSAGE_REPLIES.setScheduledStartTimeAsMilli( 1 );
        LDBC_UPDATE_1_ADD_PERSON.setScheduledStartTimeAsMilli( 1 );
        LDBC_UPDATE_2_ADD_POST_LIKE.setScheduledStartTimeAsMilli( 1 );
        LDBC_UPDATE_3_ADD_COMMENT_LIKE.setScheduledStartTimeAsMilli( 1 );
        LDBC_UPDATE_4_ADD_FORUM.setScheduledStartTimeAsMilli( 1 );
        LDBC_UPDATE_5_ADD_FORUM_MEMBERSHIP.setScheduledStartTimeAsMilli( 1 );
        LDBC_UPDATE_6_ADD_POST.setScheduledStartTimeAsMilli( 1 );
        LDBC_UPDATE_7_ADD_COMMENT.setScheduledStartTimeAsMilli( 1 );
        LDBC_UPDATE_8_ADD_FRIENDSHIP.setScheduledStartTimeAsMilli( 1 );
    }

    final GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) );
    final int highestExpectedRuntimeDurationAsNano = 10000;
    final Long[] durations =
            Lists.newArrayList( gf.limit( gf.incrementing( 1l, 1l ), highestExpectedRuntimeDurationAsNano ) )
                    .toArray( new Long[highestExpectedRuntimeDurationAsNano] );
    final TimeSource timeSource = new SystemTimeSource();
    final TimeUnit timeUnit = TimeUnit.NANOSECONDS;
    final int[] operationTypes = new int[]{
            LdbcQuery1.TYPE,
            LdbcQuery2.TYPE,
            LdbcQuery3.TYPE,
            LdbcQuery4.TYPE,
            LdbcQuery5.TYPE,
            LdbcQuery6.TYPE,
            LdbcQuery7.TYPE,
            LdbcQuery8.TYPE,
            LdbcQuery9.TYPE,
            LdbcQuery10.TYPE,
            LdbcQuery11.TYPE,
            LdbcQuery12.TYPE,
            LdbcQuery13.TYPE,
            LdbcQuery14.TYPE,
            LdbcShortQuery1PersonProfile.TYPE,
            LdbcShortQuery2PersonPosts.TYPE,
            LdbcShortQuery3PersonFriends.TYPE,
            LdbcShortQuery4MessageContent.TYPE,
            LdbcShortQuery5MessageCreator.TYPE,
            LdbcShortQuery6MessageForum.TYPE,
            LdbcShortQuery7MessageReplies.TYPE,
            LdbcUpdate1AddPerson.TYPE,
            LdbcUpdate2AddPostLike.TYPE,
            LdbcUpdate3AddCommentLike.TYPE,
            LdbcUpdate4AddForum.TYPE,
            LdbcUpdate5AddForumMembership.TYPE,
            LdbcUpdate6AddPost.TYPE,
            LdbcUpdate7AddComment.TYPE,
            LdbcUpdate8AddFriendship.TYPE
    };
    final int resultCode = 0;
    final int operationTypeCount = operationTypes.length;
    int benchmarkOperationCount = 1000000000;

        /*
        --- BASE LINE ---
            Completed 1,000,000,000 operations in 00:03.817.000 (m:s.ms.us) --> 261,985,853 op/s

        --- HdrHistogram ---
            Completed 1,000,000,000 operations in 00:07.750.000 (m:s.ms.us) --> 129,032,258 op/s

        --- ContinuousMetricManager ---
            Completed 1,000,000,000 operations in 00:08.098.000 (m:s.ms.us) --> 123,487,281 op/s

        --- OperationTypeMetricsManager ---
            Completed 1,000,000,000 operations in 00:08.088.000 (m:s.ms.us) --> 123,639,960 op/s

        --- MetricsManager ---
            Completed 1,000,000,000 operations in 00:20.577.000 (m:s.ms.us) --> 48,597,949 op/s

        ========================================================================================
        --- DisruptorMetricsCollectionEvent Creation BASE LINE ---
            Completed 1,000,000,000 operations in 00:12.995.000 (m:s.ms.us) --> 76,952,674 op/s

        --- DisruptorEventHandler ---
            Completed 1,000,000,000 operations in 00:54.530.000 (m:s.ms.us) --> 18,338,529 op/s

        --- DisruptorConcurrentMetricsService - LiteBlockingWaitStrategy ---
            Completed 1,000,000,000 operations in 01:15.957.000 (m:s.ms.us) --> 13,165,344 op/s
        ========================================================================================

        ========================================================================================
        --- OperationResultReport->ThreadedQueuedMetricsCollectionEvent Creation BASE LINE ---
            Completed 1,000,000,000 operations in 00:12.054.000 (m:s.ms.us) --> 82,960,013 op/s
            Completed 1,000,000,000 operations in 00:04.981.000 (m:s.ms.us) --> 200,762,899 op/s

        --- ThreadedQueuedConcurrentMetricsService onEvent ---
            Completed 1,000,000,000 operations in 00:57.802.000 (m:s.ms.us) --> 17,300,439 op/s
            Completed 1,000,000,000 operations in 00:40.742.000 (m:s.ms.us) --> 24,544,696 op/s

        --- ThreadedQueuedConcurrentMetricsService Non-Blocking Bounded ---
            Completed 1,000,000,000 operations in 00:48.541.000 (m:s.ms.us) --> 20,601,141 op/s
            Completed 1,000,000,000 operations in 00:37.676.000 (m:s.ms.us) --> 26,542,096 op/s
        ========================================================================================

        */

    @Test
    public void maximumThroughputBaseline() throws MetricsCollectionException
    {
        final long benchmarkStartTime = System.currentTimeMillis();
        for ( int startTime = 0; startTime < benchmarkOperationCount; startTime++ )
        {
            int operationTypeIndex = startTime % operationTypeCount;
            int operationType = operationTypes[operationTypeIndex];
            long duration = durations[startTime % highestExpectedRuntimeDurationAsNano];
            // baseline
        }
        final long benchmarkFinishTime = System.currentTimeMillis();
        final long benchmarkDurationMs = benchmarkFinishTime - benchmarkStartTime;

        final TemporalUtil temporalUtil = new TemporalUtil();
        final DecimalFormat decimalFormat = new DecimalFormat( "###,###,###,###,###,##0" );
        final double operationsPerMs = benchmarkOperationCount / (double) benchmarkDurationMs;
        final double operationsPerSecond = operationsPerMs * 1000;

        System.out.println(
                format(
                        "Completed %s operations in %s --> %s op/s",
                        decimalFormat.format( benchmarkOperationCount ),
                        temporalUtil.milliDurationToString( benchmarkDurationMs ),
                        decimalFormat.format( operationsPerSecond )
                )
        );
    }

    @Test
    public void maximumThroughputHdrHistogram() throws MetricsCollectionException
    {
        final int numberOfSignificantDigits = 4;
        final Histogram histogram = new Histogram( 1, highestExpectedRuntimeDurationAsNano, numberOfSignificantDigits );
        final long benchmarkStartTime = System.currentTimeMillis();
        for ( int startTime = 0; startTime < benchmarkOperationCount; startTime++ )
        {
            int operationTypeIndex = startTime % operationTypeCount;
            int operationType = operationTypes[operationTypeIndex];
            long duration = durations[startTime % highestExpectedRuntimeDurationAsNano];
            histogram.recordValue( duration );
        }
        final long benchmarkFinishTime = System.currentTimeMillis();
        final long benchmarkDurationMs = benchmarkFinishTime - benchmarkStartTime;

        final TemporalUtil temporalUtil = new TemporalUtil();
        final DecimalFormat decimalFormat = new DecimalFormat( "###,###,###,###,###,##0" );
        final double operationsPerMs = benchmarkOperationCount / (double) benchmarkDurationMs;
        final double operationsPerSecond = operationsPerMs * 1000;

        System.out.println(
                format(
                        "Completed %s operations in %s --> %s op/s",
                        decimalFormat.format( benchmarkOperationCount ),
                        temporalUtil.milliDurationToString( benchmarkDurationMs ),
                        decimalFormat.format( operationsPerSecond )
                )
        );
    }

    @Test
    public void maximumThroughputContinuousMetricManager() throws MetricsCollectionException
    {
        final String name = "ContinuousMetricManager";
        final int numberOfSignificantDigits = 4;
        final ContinuousMetricManager continuousMetricManager = new ContinuousMetricManager(
                name,
                TimeUnit.NANOSECONDS,
                highestExpectedRuntimeDurationAsNano,
                numberOfSignificantDigits
        );
        final long benchmarkStartTime = System.currentTimeMillis();
        for ( int startTime = 0; startTime < benchmarkOperationCount; startTime++ )
        {
            int operationTypeIndex = startTime % operationTypeCount;
            int operationType = operationTypes[operationTypeIndex];
            long duration = durations[startTime % highestExpectedRuntimeDurationAsNano];
            continuousMetricManager.addMeasurement( duration );
        }
        final long benchmarkFinishTime = System.currentTimeMillis();
        final long benchmarkDurationMs = benchmarkFinishTime - benchmarkStartTime;

        final TemporalUtil temporalUtil = new TemporalUtil();
        final DecimalFormat decimalFormat = new DecimalFormat( "###,###,###,###,###,##0" );
        final double operationsPerMs = benchmarkOperationCount / (double) benchmarkDurationMs;
        final double operationsPerSecond = operationsPerMs * 1000;

        System.out.println(
                format(
                        "Completed %s operations in %s --> %s op/s",
                        decimalFormat.format( benchmarkOperationCount ),
                        temporalUtil.milliDurationToString( benchmarkDurationMs ),
                        decimalFormat.format( operationsPerSecond )
                )
        );
    }

    @Test
    public void maximumThroughputOperationTypeMetricsManager() throws MetricsCollectionException
    {
        final String name = "OperationTypeMetricsManager";
        final OperationTypeMetricsManager operationTypeMetricsManager = new OperationTypeMetricsManager(
                name,
                timeUnit,
                highestExpectedRuntimeDurationAsNano,
                LOGGING_SERVICE_FACTORY
        );
        final long benchmarkStartTime = System.currentTimeMillis();
        for ( int startTime = 0; startTime < benchmarkOperationCount; startTime++ )
        {
            int operationTypeIndex = startTime % operationTypeCount;
            int operationType = operationTypes[operationTypeIndex];
            long duration = durations[startTime % highestExpectedRuntimeDurationAsNano];
            operationTypeMetricsManager.measure( duration );
        }
        operationTypeMetricsManager.snapshot();
        final long benchmarkFinishTime = System.currentTimeMillis();
        final long benchmarkDurationMs = benchmarkFinishTime - benchmarkStartTime;

        final TemporalUtil temporalUtil = new TemporalUtil();
        final DecimalFormat decimalFormat = new DecimalFormat( "###,###,###,###,###,##0" );
        final double operationsPerMs = benchmarkOperationCount / (double) benchmarkDurationMs;
        final double operationsPerSecond = operationsPerMs * 1000;

        System.out.println(
                format(
                        "Completed %s operations in %s --> %s op/s",
                        decimalFormat.format( benchmarkOperationCount ),
                        temporalUtil.milliDurationToString( benchmarkDurationMs ),
                        decimalFormat.format( operationsPerSecond )
                )
        );
    }

    @Test
    public void maximumThroughputMetricsManager() throws MetricsCollectionException
    {
        final MetricsManager metricsManager = new MetricsManager(
                timeSource,
                timeUnit,
                highestExpectedRuntimeDurationAsNano,
                LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping(),
                LOGGING_SERVICE_FACTORY
        );
        final long benchmarkStartTime = System.currentTimeMillis();
        for ( int startTime = 0; startTime < benchmarkOperationCount; startTime++ )
        {
            int operationTypeIndex = startTime % operationTypeCount;
            int operationType = operationTypes[operationTypeIndex];
            long duration = durations[startTime % highestExpectedRuntimeDurationAsNano];
            metricsManager.measure( startTime, duration, operationType );
        }
        final WorkloadResultsSnapshot snapshot = metricsManager.snapshot();
        final long benchmarkFinishTime = System.currentTimeMillis();
        final long benchmarkDurationMs = benchmarkFinishTime - benchmarkStartTime;

        final TemporalUtil temporalUtil = new TemporalUtil();
        final DecimalFormat decimalFormat = new DecimalFormat( "###,###,###,###,###,##0" );
        final double operationsPerMs = benchmarkOperationCount / (double) benchmarkDurationMs;
        final double operationsPerSecond = operationsPerMs * 1000;

        System.out.println(
                format(
                        "Completed %s operations in %s --> %s op/s",
                        decimalFormat.format( benchmarkOperationCount ),
                        temporalUtil.milliDurationToString( benchmarkDurationMs ),
                        decimalFormat.format( operationsPerSecond )
                )
        );

        final WorkloadMetricsFormatter formatter = new SimpleDetailedWorkloadMetricsFormatter();
        System.out.println( formatter.format( snapshot ) );
    }

    @Test
    public void maximumThroughputDisruptorEventHandlerBaseline() throws Exception
    {
        final DisruptorJavolutionMetricsEvent event = new DisruptorJavolutionMetricsEvent();
        event.setEventType( DisruptorJavolutionMetricsEvent.SUBMIT_RESULT );
        event.setResultCode( resultCode );
        final long benchmarkStartTime = System.currentTimeMillis();
        for ( int startTime = 0; startTime < benchmarkOperationCount; startTime++ )
        {
            int operationTypeIndex = startTime % operationTypeCount;
            int operationType = operationTypes[operationTypeIndex];
            long duration = durations[startTime % highestExpectedRuntimeDurationAsNano];
            event.setOperationType( operationType );
            event.setScheduledStartTimeAsMilli( startTime );
            event.setActualStartTimeAsMilli( startTime );
            event.setRunDurationAsNano( duration );
            // do nothing
        }
        final long benchmarkFinishTime = System.currentTimeMillis();
        final long benchmarkDurationMs = benchmarkFinishTime - benchmarkStartTime;

        final TemporalUtil temporalUtil = new TemporalUtil();
        final DecimalFormat decimalFormat = new DecimalFormat( "###,###,###,###,###,##0" );
        final double operationsPerMs = benchmarkOperationCount / (double) benchmarkDurationMs;
        final double operationsPerSecond = operationsPerMs * 1000;

        System.out.println(
                format(
                        "Completed %s operations in %s --> %s op/s",
                        decimalFormat.format( benchmarkOperationCount ),
                        temporalUtil.milliDurationToString( benchmarkDurationMs ),
                        decimalFormat.format( operationsPerSecond )
                )
        );
    }

    @Test
    public void maximumThroughputDisruptorEventHandler() throws Exception
    {
        final DisruptorJavolutionMetricsEvent event = new DisruptorJavolutionMetricsEvent();
        event.setEventType( DisruptorJavolutionMetricsEvent.SUBMIT_RESULT );
        event.setResultCode( resultCode );
        final ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        final DisruptorJavolutionMetricsEventHandler eventHandler = new DisruptorJavolutionMetricsEventHandler(
                errorReporter,
                null,
                timeUnit,
                timeSource,
                highestExpectedRuntimeDurationAsNano,
                LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping(),
                LOGGING_SERVICE_FACTORY
        );
        final long benchmarkStartTime = System.currentTimeMillis();
        for ( int startTime = 0; startTime < benchmarkOperationCount; startTime++ )
        {
            int operationTypeIndex = startTime % operationTypeCount;
            int operationType = operationTypes[operationTypeIndex];
            long duration = durations[startTime % highestExpectedRuntimeDurationAsNano];
            event.setOperationType( operationType );
            event.setScheduledStartTimeAsMilli( startTime );
            event.setActualStartTimeAsMilli( startTime );
            event.setRunDurationAsNano( duration );
            eventHandler.onEvent( event, 1, true );
        }
        AtomicStampedReference<WorkloadResultsSnapshot> resultsSnapshotReference = eventHandler.resultsSnapshot();
        int oldStamp = resultsSnapshotReference.getStamp();
        DisruptorJavolutionMetricsEvent snapshotEvent = new DisruptorJavolutionMetricsEvent();
        snapshotEvent.setEventType( DisruptorJavolutionMetricsEvent.WORKLOAD_RESULT );
        eventHandler.onEvent( snapshotEvent, 1, true );
        while ( resultsSnapshotReference.getStamp() <= oldStamp )
        {
            LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 1 ) );
        }
        final WorkloadResultsSnapshot snapshot = eventHandler.resultsSnapshot().getReference();
        final long benchmarkFinishTime = System.currentTimeMillis();
        final long benchmarkDurationMs = benchmarkFinishTime - benchmarkStartTime;

        final TemporalUtil temporalUtil = new TemporalUtil();
        final DecimalFormat decimalFormat = new DecimalFormat( "###,###,###,###,###,##0" );
        final double operationsPerMs = benchmarkOperationCount / (double) benchmarkDurationMs;
        final double operationsPerSecond = operationsPerMs * 1000;

        System.out.println(
                format(
                        "Completed %s operations in %s --> %s op/s",
                        decimalFormat.format( benchmarkOperationCount ),
                        temporalUtil.milliDurationToString( benchmarkDurationMs ),
                        decimalFormat.format( operationsPerSecond )
                )
        );

        final WorkloadMetricsFormatter formatter = new SimpleDetailedWorkloadMetricsFormatter();
        System.out.println( formatter.format( snapshot ) );
    }

    @Test
    public void maximumThroughputThreadedQueuedMetricsServiceBaseLine() throws MetricsCollectionException
    {
        final long benchmarkStartTime = System.currentTimeMillis();
        for ( int startTime = 0; startTime < benchmarkOperationCount; startTime++ )
        {
            int operationTypeIndex = startTime % operationTypeCount;
            long duration = durations[startTime % highestExpectedRuntimeDurationAsNano];
            int operationType = operationTypes[operationTypeIndex];
            ThreadedQueuedMetricsEvent event = new ThreadedQueuedMetricsEvent.SubmitOperationResult(
                    operationType,
                    startTime,
                    startTime,
                    duration,
                    resultCode
            );
            // do nothing
        }
        final long benchmarkFinishTime = System.currentTimeMillis();
        final long benchmarkDurationMs = benchmarkFinishTime - benchmarkStartTime;

        final TemporalUtil temporalUtil = new TemporalUtil();
        final DecimalFormat decimalFormat = new DecimalFormat( "###,###,###,###,###,##0" );
        final double operationsPerMs = benchmarkOperationCount / (double) benchmarkDurationMs;
        final double operationsPerSecond = operationsPerMs * 1000;

        System.out.println(
                format(
                        "Completed %s operations in %s --> %s op/s",
                        decimalFormat.format( benchmarkOperationCount ),
                        temporalUtil.milliDurationToString( benchmarkDurationMs ),
                        decimalFormat.format( operationsPerSecond )
                )
        );
    }

    @Test
    public void maximumThroughputThreadedQueuedMetricsServiceHandler() throws MetricsCollectionException, IOException
    {
        final ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ThreadedQueuedMetricsServiceThread metricsServiceThread = new ThreadedQueuedMetricsServiceThread(
                errorReporter,
                DefaultQueues.<ThreadedQueuedMetricsEvent>newNonBlockingBounded( 10000 ),
                null,
                timeSource,
                timeUnit,
                highestExpectedRuntimeDurationAsNano,
                LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping(),
                LOGGING_SERVICE_FACTORY
        );
        final long benchmarkStartTime = System.currentTimeMillis();
        for ( int startTime = 0; startTime < benchmarkOperationCount; startTime++ )
        {
            int operationTypeIndex = startTime % operationTypeCount;
            long duration = durations[startTime % highestExpectedRuntimeDurationAsNano];
            int operationType = operationTypes[operationTypeIndex];
            ThreadedQueuedMetricsEvent event = new ThreadedQueuedMetricsEvent.SubmitOperationResult(
                    operationType,
                    startTime,
                    startTime,
                    duration,
                    resultCode
            );
            metricsServiceThread.onEvent( event );
        }
        // get snapshot
        final long benchmarkFinishTime = System.currentTimeMillis();
        final long benchmarkDurationMs = benchmarkFinishTime - benchmarkStartTime;

        final TemporalUtil temporalUtil = new TemporalUtil();
        final DecimalFormat decimalFormat = new DecimalFormat( "###,###,###,###,###,##0" );
        final double operationsPerMs = benchmarkOperationCount / (double) benchmarkDurationMs;
        final double operationsPerSecond = operationsPerMs * 1000;

        System.out.println(
                format(
                        "Completed %s operations in %s --> %s op/s",
                        decimalFormat.format( benchmarkOperationCount ),
                        temporalUtil.milliDurationToString( benchmarkDurationMs ),
                        decimalFormat.format( operationsPerSecond )
                )
        );
    }

    @Test
    public void maximumThroughputThreadedQueuedMetricsServiceNonBlockingBounded() throws MetricsCollectionException
    {
        final ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        MetricsService metricsService = ThreadedQueuedMetricsService.newInstanceUsingNonBlockingBoundedQueue(
                timeSource,
                errorReporter,
                timeUnit,
                highestExpectedRuntimeDurationAsNano,
                null,
                LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping(),
                LOGGING_SERVICE_FACTORY
        );
        MetricsService.MetricsServiceWriter metricsServiceWriter = metricsService.getWriter();
        final long benchmarkStartTime = System.currentTimeMillis();
        for ( int startTime = 0; startTime < benchmarkOperationCount; startTime++ )
        {
            int operationTypeIndex = startTime % operationTypeCount;
            long duration = durations[startTime % highestExpectedRuntimeDurationAsNano];
            int operationType = operationTypes[operationTypeIndex];
            metricsServiceWriter.submitOperationResult( operationType, startTime, startTime, duration, resultCode );
        }
        final WorkloadResultsSnapshot snapshot = metricsServiceWriter.results();
        final long benchmarkFinishTime = System.currentTimeMillis();
        final long benchmarkDurationMs = benchmarkFinishTime - benchmarkStartTime;

        final TemporalUtil temporalUtil = new TemporalUtil();
        final DecimalFormat decimalFormat = new DecimalFormat( "###,###,###,###,###,##0" );
        final double operationsPerMs = benchmarkOperationCount / (double) benchmarkDurationMs;
        final double operationsPerSecond = operationsPerMs * 1000;

        System.out.println(
                format(
                        "Completed %s operations in %s --> %s op/s",
                        decimalFormat.format( benchmarkOperationCount ),
                        temporalUtil.milliDurationToString( benchmarkDurationMs ),
                        decimalFormat.format( operationsPerSecond )
                )
        );

        final WorkloadMetricsFormatter formatter = new SimpleDetailedWorkloadMetricsFormatter();
        System.out.println( formatter.format( snapshot ) );
        metricsService.shutdown();
    }

    @Test
    public void maximumThroughputJavolutionDisruptorMetricsService() throws MetricsCollectionException
    {
        final ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        MetricsService metricsService = new DisruptorJavolutionMetricsService(
                timeSource,
                errorReporter,
                timeUnit,
                highestExpectedRuntimeDurationAsNano,
                null,
                LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping(),
                LOGGING_SERVICE_FACTORY
        );
        MetricsService.MetricsServiceWriter metricsServiceWriter = metricsService.getWriter();
        final long benchmarkStartTime = System.currentTimeMillis();
        for ( int startTime = 0; startTime < benchmarkOperationCount; startTime++ )
        {
            int operationTypeIndex = startTime % operationTypeCount;
            long duration = durations[startTime % highestExpectedRuntimeDurationAsNano];
            int operationType = operationTypes[operationTypeIndex];
            metricsServiceWriter.submitOperationResult( operationType, startTime, startTime, duration, resultCode );
        }
        final WorkloadResultsSnapshot snapshot = metricsServiceWriter.results();
        final long benchmarkFinishTime = System.currentTimeMillis();
        final long benchmarkDurationMs = benchmarkFinishTime - benchmarkStartTime;

        final TemporalUtil temporalUtil = new TemporalUtil();
        final DecimalFormat decimalFormat = new DecimalFormat( "###,###,###,###,###,##0" );
        final double operationsPerMs = benchmarkOperationCount / (double) benchmarkDurationMs;
        final double operationsPerSecond = operationsPerMs * 1000;

        System.out.println(
                format(
                        "Completed %s operations in %s --> %s op/s",
                        decimalFormat.format( benchmarkOperationCount ),
                        temporalUtil.milliDurationToString( benchmarkDurationMs ),
                        decimalFormat.format( operationsPerSecond )
                )
        );

        final WorkloadMetricsFormatter formatter = new SimpleDetailedWorkloadMetricsFormatter();
        System.out.println( formatter.format( snapshot ) );
        metricsService.shutdown();
    }

    @Test
    public void maximumThroughputSbeDisruptorMetricsService() throws MetricsCollectionException
    {
        final ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        MetricsService metricsService = new DisruptorSbeMetricsService(
                timeSource,
                errorReporter,
                timeUnit,
                highestExpectedRuntimeDurationAsNano,
                null,
                LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping(),
                LOGGING_SERVICE_FACTORY
        );
        MetricsService.MetricsServiceWriter metricsServiceWriter = metricsService.getWriter();
        final long benchmarkStartTime = System.currentTimeMillis();
        for ( int startTime = 0; startTime < benchmarkOperationCount; startTime++ )
        {
            int operationTypeIndex = startTime % operationTypeCount;
            long duration = durations[startTime % highestExpectedRuntimeDurationAsNano];
            int operationType = operationTypes[operationTypeIndex];
            metricsServiceWriter.submitOperationResult( operationType, startTime, startTime, duration, resultCode );
        }
        final WorkloadResultsSnapshot snapshot = metricsServiceWriter.results();
        final long benchmarkFinishTime = System.currentTimeMillis();
        final long benchmarkDurationMs = benchmarkFinishTime - benchmarkStartTime;

        final TemporalUtil temporalUtil = new TemporalUtil();
        final DecimalFormat decimalFormat = new DecimalFormat( "###,###,###,###,###,##0" );
        final double operationsPerMs = benchmarkOperationCount / (double) benchmarkDurationMs;
        final double operationsPerSecond = operationsPerMs * 1000;

        System.out.println(
                format(
                        "Completed %s operations in %s --> %s op/s",
                        decimalFormat.format( benchmarkOperationCount ),
                        temporalUtil.milliDurationToString( benchmarkDurationMs ),
                        decimalFormat.format( operationsPerSecond )
                )
        );

        final WorkloadMetricsFormatter formatter = new SimpleDetailedWorkloadMetricsFormatter();
        System.out.println( formatter.format( snapshot ) );
        metricsService.shutdown();
    }

    /*
    Disruptor - Javolution - BlockingWaitStrategy
        DisruptorConcurrentMetricsService 1 threads 1,000,000,000 operations in 01:40.084.000 (m:s.ms.us) --> 9,991,
        607 op/s
        DisruptorConcurrentMetricsService 2 threads 1,000,000,000 operations in 01:19.931.000 (m:s.ms.us) --> 12,510,
        791 op/s
        DisruptorConcurrentMetricsService 4 threads 1,000,000,000 operations in 02:37.760.000 (m:s.ms.us) --> 6,338,
        742 op/s
        DisruptorConcurrentMetricsService 8 threads 1,000,000,000 operations in 02:47.957.000 (m:s.ms.us) --> 5,953,
        905 op/s
    Disruptor - Javolution - LiteBlockingWaitStrategy
        DisruptorConcurrentMetricsService 1 threads 1,000,000,000 operations in 01:18.993.000 (m:s.ms.us) --> 12,659,
        350 op/s
        DisruptorConcurrentMetricsService 2 threads 1,000,000,000 operations in 01:33.577.000 (m:s.ms.us) --> 10,686,
        387 op/s
        DisruptorConcurrentMetricsService 4 threads 1,000,000,000 operations in 01:28.682.000 (m:s.ms.us) --> 11,276,
        245 op/s
        DisruptorConcurrentMetricsService 8 threads 1,000,000,000 operations in 01:58.724.000 (m:s.ms.us) --> 8,422,
        897 op/s
    Disruptor - Javolution - SleepingWaitStrategy
        DisruptorConcurrentMetricsService 1 threads 1,000,000,000 operations in 01:36.366.000 (m:s.ms.us) --> 10,377,
        104 op/s
        DisruptorConcurrentMetricsService 2 threads 1,000,000,000 operations in 01:20.450.000 (m:s.ms.us) --> 12,430,
        081 op/s
        DisruptorConcurrentMetricsService 4 threads 1,000,000,000 operations in 01:21.367.000 (m:s.ms.us) --> 12,289,
        995 op/s
        DisruptorConcurrentMetricsService 8 threads 1,000,000,000 operations in 01:40.621.000 (m:s.ms.us) --> 9,938,
        283 op/s

    Disruptor - SBE - LiteBlockingWaitStrategy

    LinkedBlockingQueue: Bounded Blocking
        ThreadedQueuedConcurrentMetricsService 1 threads 1,000,000,000 operations in 03:23.162.000 (m:s.ms.us) --> 4,
        922,180 op/s
        ThreadedQueuedConcurrentMetricsService 2 threads 1,000,000,000 operations in 04:55.122.000 (m:s.ms.us) --> 3,
        388,429 op/s
        ThreadedQueuedConcurrentMetricsService 4 threads 1,000,000,000 operations in 04:23.480.000 (m:s.ms.us) --> 3,
        795,354 op/s
        ThreadedQueuedConcurrentMetricsService 8 threads 1,000,000,000 operations in 04:28.738.000 (m:s.ms.us) --> 3,
        721,096 op/s

    ArrayBlockingQueue: Bounded Blocking
        ThreadedQueuedConcurrentMetricsService 1 threads 1,000,000,000 operations in 04:21.546.000 (m:s.ms.us) --> 3,
        823,419 op/s
        ThreadedQueuedConcurrentMetricsService 2 threads 1,000,000,000 operations in 02:10.107.000 (m:s.ms.us) --> 7,
        685,982 op/s
        ThreadedQueuedConcurrentMetricsService 4 threads 1,000,000,000 operations in 02:03.633.000 (m:s.ms.us) --> 8,
        088,455 op/s
        ThreadedQueuedConcurrentMetricsService 8 threads 1,000,000,000 operations in 02:32.904.000 (m:s.ms.us) --> 6,
        540,051 op/s

    JCTools MPSC: Bounded Non-Blocking - LockSupport.parkNanos(1)
        ThreadedQueuedConcurrentMetricsService 1 threads 1,000,000,000 operations in 00:30.147.000 (m:s.ms.us) -->
        33,170,796 op/s
        ThreadedQueuedConcurrentMetricsService 2 threads 1,000,000,000 operations in 01:16.771.000 (m:s.ms.us) -->
        13,025,752 op/s
        ThreadedQueuedConcurrentMetricsService 4 threads 1,000,000,000 operations in 01:21.476.000 (m:s.ms.us) -->
        12,273,553 op/s
        ThreadedQueuedConcurrentMetricsService 8 threads 1,000,000,000 operations in 02:01.303.000 (m:s.ms.us) --> 8,
        243,819 op/s


     */
    @Test
    public void multiThreadedTest() throws InterruptedException, MetricsCollectionException
    {
        int threadCount = 1;
        int operationCount = 1000000000;

        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        AtomicLong processedOperationCount = new AtomicLong( 0 );
        CountDownLatch readyLatch = new CountDownLatch( threadCount );
        CountDownLatch startLatch = new CountDownLatch( 1 );
        CountDownLatch stopLatch = new CountDownLatch( threadCount );

//        ConcurrentMetricsService metricsService = new DisruptorConcurrentMetricsService(
//                timeSource,
//                errorReporter,
//                timeUnit,
//                highestExpectedRuntimeDurationAsNano,
//                null,
//                LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping()
//        );

        MetricsService metricsService = new DisruptorSbeMetricsService(
                timeSource,
                errorReporter,
                timeUnit,
                highestExpectedRuntimeDurationAsNano,
                null,
                LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping(),
                LOGGING_SERVICE_FACTORY
        );

//        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService
// .newInstanceUsingNonBlockingBoundedQueue(
//                timeSource,
//                errorReporter,
//                timeUnit,
//                highestExpectedRuntimeDurationAsNano,
//                null,
//                LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping()
//        );

        for ( int i = 0; i < threadCount; i++ )
        {
            new MetricsServiceWriterThread(
                    metricsService.getWriter(),
                    operationCount / threadCount,
                    readyLatch,
                    startLatch,
                    stopLatch,
                    processedOperationCount
            ).start();
        }
        readyLatch.await();
        startLatch.countDown();
        long startTime = System.currentTimeMillis();
        stopLatch.await();
        final WorkloadResultsSnapshot snapshot = metricsService.getWriter().results();
        long finishTime = System.currentTimeMillis();

        long benchmarkDurationMs = finishTime - startTime;
        final TemporalUtil temporalUtil = new TemporalUtil();
        final DecimalFormat decimalFormat = new DecimalFormat( "###,###,###,###,###,##0" );
        final double operationsPerMs = processedOperationCount.get() / (double) benchmarkDurationMs;
        final double operationsPerSecond = operationsPerMs * 1000;

        System.out.println(
                format(
                        "%s %s threads %s operations in %s --> %s op/s",
                        metricsService.getClass().getSimpleName(),
                        threadCount,
                        decimalFormat.format( processedOperationCount.get() ),
                        temporalUtil.milliDurationToString( benchmarkDurationMs ),
                        decimalFormat.format( operationsPerSecond )
                )
        );

        final WorkloadMetricsFormatter formatter = new SimpleDetailedWorkloadMetricsFormatter();
        System.out.println( formatter.format( snapshot ) );
        metricsService.shutdown();

    }

    public class MetricsServiceWriterThread extends Thread
    {
        private final MetricsService.MetricsServiceWriter metricsServiceWriter;
        private final int operationCount;
        private final CountDownLatch readyLatch;
        private final CountDownLatch startLatch;
        private final CountDownLatch stopLatch;
        private final AtomicLong processedOperationCount;

        public MetricsServiceWriterThread( MetricsService.MetricsServiceWriter metricsServiceWriter,
                int operationCount,
                CountDownLatch readyLatch,
                CountDownLatch startLatch,
                CountDownLatch stopLatch,
                AtomicLong processedOperationCount )
        {
            super( MetricsServiceWriterThread.class.getSimpleName() + "-" + System.currentTimeMillis() );
            this.metricsServiceWriter = metricsServiceWriter;
            this.operationCount = operationCount;
            this.readyLatch = readyLatch;
            this.startLatch = startLatch;
            this.stopLatch = stopLatch;
            this.processedOperationCount = processedOperationCount;
        }

        @Override
        public void run()
        {
            readyLatch.countDown();
            try
            {
                startLatch.await();
            }
            catch ( InterruptedException e )
            {
                e.printStackTrace();
                return;
            }
            for ( int startTime = 0; startTime < operationCount; startTime++ )
            {
                int operationTypeIndex = startTime % operationTypeCount;
                long duration = durations[startTime % highestExpectedRuntimeDurationAsNano];
                int operationType = operationTypes[operationTypeIndex];
                try
                {
                    metricsServiceWriter
                            .submitOperationResult( operationType, startTime, startTime, duration, resultCode );
                }
                catch ( MetricsCollectionException e )
                {
                    e.printStackTrace();
                    return;
                }
            }
            processedOperationCount.addAndGet( operationCount );
            stopLatch.countDown();
        }
    }
}
