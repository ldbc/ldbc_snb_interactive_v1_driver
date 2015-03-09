package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationInstances;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationResultInstances;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class LdbcSnbShortReadGeneratorTest {

    @Test
    public void shouldReturnExpectedOperationsWhenAllEnabled() throws WorkloadException {
        // Given
        double initialProbability = Double.MAX_VALUE;
        double probabilityDegradationFactor = 0.1;
        long interleaveAsMilli = 100;
        Set<Class> enabledShortReadOperationTypes = Sets.<Class>newHashSet(
                LdbcShortQuery1PersonProfile.class,
                LdbcShortQuery2PersonPosts.class,
                LdbcShortQuery3PersonFriends.class,
                LdbcShortQuery4MessageContent.class,
                LdbcShortQuery5MessageCreator.class,
                LdbcShortQuery6MessageForum.class,
                LdbcShortQuery7MessageReplies.class
        );
        double compressionRatio = 1.0;
        EvictingQueue<Long> personIdBuffer = EvictingQueue.create(100);
        personIdBuffer.addAll(Lists.newArrayList(1l, 2l, 3l, 4l, 5l));
        EvictingQueue<Long> messageIdBuffer = EvictingQueue.create(100);
        messageIdBuffer.addAll(Lists.newArrayList(1l, 2l, 3l, 4l, 5l));
        RandomDataGeneratorFactory randomFactory = new RandomDataGeneratorFactory(1l);
        LdbcSnbShortReadGenerator shortReadGenerator = new LdbcSnbShortReadGenerator(
                initialProbability,
                probabilityDegradationFactor,
                interleaveAsMilli,
                enabledShortReadOperationTypes,
                compressionRatio,
                personIdBuffer,
                messageIdBuffer,
                randomFactory
        );

        // When
        long correctedInterleaveAsMilli = Math.round(interleaveAsMilli * compressionRatio);
        double state = shortReadGenerator.initialState();
        assertThat(state, is(initialProbability));
        long previousScheduledStartTime = DummyLdbcSnbInteractiveOperationInstances.read1().scheduledStartTimeAsMilli();
        Operation operation = shortReadGenerator.nextOperation(
                state,
                DummyLdbcSnbInteractiveOperationInstances.read1(),
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.read1Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.read1Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.read1Result()
                )
        );

        // Then
        // round robbin will choose short read 1 before short read 4
        assertThat(operation.type(), equalTo(LdbcShortQuery1PersonProfile.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();
        assertThat(state, is(initialProbability));

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.short1Result()
                )
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery2PersonPosts.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.short2Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.short2Result()
                )
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery3PersonFriends.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability - probabilityDegradationFactor));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.short3Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.short3Result()
                )
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery4MessageContent.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability - probabilityDegradationFactor));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.short4Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.short4Result()
                )
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery5MessageCreator.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability - probabilityDegradationFactor));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                DummyLdbcSnbInteractiveOperationResultInstances.short5Result()
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery6MessageForum.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability - probabilityDegradationFactor));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                DummyLdbcSnbInteractiveOperationResultInstances.short6Result()
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery7MessageReplies.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability - probabilityDegradationFactor - probabilityDegradationFactor));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.short7Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.short7Result()
                )
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery1PersonProfile.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability - probabilityDegradationFactor - probabilityDegradationFactor));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.short1Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.short1Result()
                )
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery2PersonPosts.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability - probabilityDegradationFactor - probabilityDegradationFactor));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.short2Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.short2Result()
                )
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery3PersonFriends.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability - probabilityDegradationFactor - probabilityDegradationFactor - probabilityDegradationFactor));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.short3Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.short3Result()
                )
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery4MessageContent.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
    }

    @Test
    public void shouldReturnExpectedOperationsWhenSomeAreDisabled1() throws WorkloadException {
        // Given
        double initialProbability = Double.MAX_VALUE;
        double probabilityDegradationFactor = 0.1;
        long interleaveAsMilli = 100;
        Set<Class> enabledShortReadOperationTypes = Sets.<Class>newHashSet(
//                LdbcShortQuery1PersonProfile.class,
                LdbcShortQuery2PersonPosts.class,
//                LdbcShortQuery3PersonFriends.class,
                LdbcShortQuery4MessageContent.class,
                LdbcShortQuery5MessageCreator.class,
//                LdbcShortQuery6MessageForum.class,
                LdbcShortQuery7MessageReplies.class
        );
        double compressionRatio = 0.9;
        EvictingQueue<Long> personIdBuffer = EvictingQueue.create(100);
        personIdBuffer.addAll(Lists.newArrayList(1l, 2l, 3l, 4l, 5l));
        EvictingQueue<Long> messageIdBuffer = EvictingQueue.create(100);
        messageIdBuffer.addAll(Lists.newArrayList(1l, 2l, 3l, 4l, 5l));
        RandomDataGeneratorFactory randomFactory = new RandomDataGeneratorFactory(1l);
        LdbcSnbShortReadGenerator shortReadGenerator = new LdbcSnbShortReadGenerator(
                initialProbability,
                probabilityDegradationFactor,
                interleaveAsMilli,
                enabledShortReadOperationTypes,
                compressionRatio,
                personIdBuffer,
                messageIdBuffer,
                randomFactory
        );

        // When
        long correctedInterleaveAsMilli = Math.round(interleaveAsMilli * compressionRatio);
        double state = shortReadGenerator.initialState();
        assertThat(state, is(initialProbability));
        long previousScheduledStartTime = DummyLdbcSnbInteractiveOperationInstances.read2().scheduledStartTimeAsMilli();
        Operation operation = shortReadGenerator.nextOperation(
                state,
                DummyLdbcSnbInteractiveOperationInstances.read2(),
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.read2Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.read2Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.read2Result()
                )
        );

        // Then
        // round robbin will choose short read 2 before short read 4
        assertThat(operation.type(), equalTo(LdbcShortQuery2PersonPosts.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();
        assertThat(state, is(initialProbability));

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability - probabilityDegradationFactor));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.short2Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.short2Result()
                )
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery4MessageContent.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability - probabilityDegradationFactor));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.short4Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.short4Result()
                )
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery5MessageCreator.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability - probabilityDegradationFactor));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                DummyLdbcSnbInteractiveOperationResultInstances.short5Result()
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery7MessageReplies.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability - probabilityDegradationFactor - probabilityDegradationFactor));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.short7Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.short7Result()
                )
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery2PersonPosts.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability - probabilityDegradationFactor - probabilityDegradationFactor - probabilityDegradationFactor));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.short2Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.short2Result()
                )
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery4MessageContent.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
    }

    @Test
    public void shouldReturnExpectedOperationsWhenSomeAreDisabled2() throws WorkloadException {
        // Given
        double initialProbability = Double.MAX_VALUE;
        double probabilityDegradationFactor = 0.1;
        long interleaveAsMilli = 100;
        Set<Class> enabledShortReadOperationTypes = Sets.<Class>newHashSet(
//                LdbcShortQuery1PersonProfile.class,
//                LdbcShortQuery2PersonPosts.class,
//                LdbcShortQuery3PersonFriends.class,
                LdbcShortQuery4MessageContent.class,
                LdbcShortQuery5MessageCreator.class,
//                LdbcShortQuery6MessageForum.class,
                LdbcShortQuery7MessageReplies.class
        );
        double compressionRatio = 2.5;
        EvictingQueue<Long> personIdBuffer = EvictingQueue.create(100);
        personIdBuffer.addAll(Lists.newArrayList(1l, 2l, 3l, 4l, 5l));
        EvictingQueue<Long> messageIdBuffer = EvictingQueue.create(100);
        messageIdBuffer.addAll(Lists.newArrayList(1l, 2l, 3l, 4l, 5l));
        RandomDataGeneratorFactory randomFactory = new RandomDataGeneratorFactory(1l);
        LdbcSnbShortReadGenerator shortReadGenerator = new LdbcSnbShortReadGenerator(
                initialProbability,
                probabilityDegradationFactor,
                interleaveAsMilli,
                enabledShortReadOperationTypes,
                compressionRatio,
                personIdBuffer,
                messageIdBuffer,
                randomFactory
        );

        // When
        long correctedInterleaveAsMilli = Math.round(interleaveAsMilli * compressionRatio);
        double state = shortReadGenerator.initialState();
        assertThat(state, is(initialProbability));
        long previousScheduledStartTime = DummyLdbcSnbInteractiveOperationInstances.read3().scheduledStartTimeAsMilli();
        Operation operation = shortReadGenerator.nextOperation(
                state,
                DummyLdbcSnbInteractiveOperationInstances.read3(),
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.read3Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.read3Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.read3Result()
                )
        );

        // Then
        assertThat(operation.type(), equalTo(LdbcShortQuery4MessageContent.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();
        assertThat(state, is(initialProbability));

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.short4Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.short4Result()
                )
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery5MessageCreator.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                DummyLdbcSnbInteractiveOperationResultInstances.short5Result()
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery7MessageReplies.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability - probabilityDegradationFactor));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.short7Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.short7Result()
                )
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery4MessageContent.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability - probabilityDegradationFactor));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.short4Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.short4Result()
                )
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery5MessageCreator.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
    }

    @Test
    public void shouldReturnExpectedOperationsWhenSomeAreDisabled3() throws WorkloadException {
        // Given
        double initialProbability = Double.MAX_VALUE;
        double probabilityDegradationFactor = 0.1;
        long interleaveAsMilli = 100;
        Set<Class> enabledShortReadOperationTypes = Sets.<Class>newHashSet(
//                LdbcShortQuery1PersonProfile.class,
//                LdbcShortQuery2PersonPosts.class,
//                LdbcShortQuery3PersonFriends.class,
//                LdbcShortQuery4MessageContent.class,
//                LdbcShortQuery5MessageCreator.class,
//                LdbcShortQuery6MessageForum.class,
                LdbcShortQuery7MessageReplies.class
        );
        double compressionRatio = 0.2;
        EvictingQueue<Long> personIdBuffer = EvictingQueue.create(100);
        personIdBuffer.addAll(Lists.newArrayList(1l, 2l, 3l, 4l, 5l));
        EvictingQueue<Long> messageIdBuffer = EvictingQueue.create(100);
        messageIdBuffer.addAll(Lists.newArrayList(1l, 2l, 3l, 4l, 5l));
        RandomDataGeneratorFactory randomFactory = new RandomDataGeneratorFactory(1l);
        LdbcSnbShortReadGenerator shortReadGenerator = new LdbcSnbShortReadGenerator(
                initialProbability,
                probabilityDegradationFactor,
                interleaveAsMilli,
                enabledShortReadOperationTypes,
                compressionRatio,
                personIdBuffer,
                messageIdBuffer,
                randomFactory
        );

        // When
        long correctedInterleaveAsMilli = Math.round(interleaveAsMilli * compressionRatio);
        double state = shortReadGenerator.initialState();
        assertThat(state, is(initialProbability));
        long previousScheduledStartTime = DummyLdbcSnbInteractiveOperationInstances.read2().scheduledStartTimeAsMilli();
        Operation operation = shortReadGenerator.nextOperation(
                state,
                DummyLdbcSnbInteractiveOperationInstances.read1(),
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.read1Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.read1Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.read1Result()
                )
        );

        // Then
        assertThat(operation.type(), equalTo(LdbcShortQuery7MessageReplies.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();
        assertThat(state, is(initialProbability));

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability - probabilityDegradationFactor));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.short7Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.short7Result()
                )
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery7MessageReplies.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability - probabilityDegradationFactor - probabilityDegradationFactor));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.short7Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.short7Result()
                )
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery7MessageReplies.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability - probabilityDegradationFactor - probabilityDegradationFactor - probabilityDegradationFactor));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.short7Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.short7Result()
                )
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery7MessageReplies.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
        previousScheduledStartTime = operation.scheduledStartTimeAsMilli();

        state = shortReadGenerator.updateState(state, operation.type());
        assertThat(state, is(initialProbability - probabilityDegradationFactor - probabilityDegradationFactor - probabilityDegradationFactor - probabilityDegradationFactor));
        operation = shortReadGenerator.nextOperation(
                state,
                operation,
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.short7Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.short7Result()
                )
        );
        assertThat(operation.type(), equalTo(LdbcShortQuery7MessageReplies.TYPE));
        assertThat(operation.scheduledStartTimeAsMilli(), is(previousScheduledStartTime + correctedInterleaveAsMilli));
    }

    @Test
    public void shouldReturnExpectedOperationsWhenAllAreDisabled() throws WorkloadException {
        // Given
        double initialProbability = Double.MAX_VALUE;
        double probabilityDegradationFactor = 0.1;
        long interleaveAsMilli = 100;
        Set<Class> enabledShortReadOperationTypes = Sets.<Class>newHashSet(
//                LdbcShortQuery1PersonProfile.class,
//                LdbcShortQuery2PersonPosts.class,
//                LdbcShortQuery3PersonFriends.class,
//                LdbcShortQuery4MessageContent.class,
//                LdbcShortQuery5MessageCreator.class,
//                LdbcShortQuery6MessageForum.class,
//                LdbcShortQuery7MessageReplies.class
        );
        double compressionRatio = 1.0;
        EvictingQueue<Long> personIdBuffer = EvictingQueue.create(100);
        personIdBuffer.addAll(Lists.newArrayList(1l, 2l, 3l, 4l, 5l));
        EvictingQueue<Long> messageIdBuffer = EvictingQueue.create(100);
        messageIdBuffer.addAll(Lists.newArrayList(1l, 2l, 3l, 4l, 5l));
        RandomDataGeneratorFactory randomFactory = new RandomDataGeneratorFactory(1l);
        LdbcSnbShortReadGenerator shortReadGenerator = new LdbcSnbShortReadGenerator(
                initialProbability,
                probabilityDegradationFactor,
                interleaveAsMilli,
                enabledShortReadOperationTypes,
                compressionRatio,
                personIdBuffer,
                messageIdBuffer,
                randomFactory
        );

        // When
        double state = shortReadGenerator.initialState();
        assertThat(state, is(initialProbability));
        Operation operation = shortReadGenerator.nextOperation(
                state,
                DummyLdbcSnbInteractiveOperationInstances.read1(),
                Lists.newArrayList(
                        DummyLdbcSnbInteractiveOperationResultInstances.read1Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.read1Result(),
                        DummyLdbcSnbInteractiveOperationResultInstances.read1Result()
                )
        );

        // Then
        assertThat(operation, is(nullValue()));
        assertThat(state, is(initialProbability));
    }
}
