package com.ldbc.driver.runtime.metrics.sbe_test;

import com.google.common.collect.Lists;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.metrics.event.MessageHeader;
import com.ldbc.driver.runtime.metrics.event.MetricsEvent;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;

public class MetricsEventSBETest {
    private static final MessageHeader MESSAGE_HEADER = new MessageHeader();
    private static final MetricsEvent EVENT = new MetricsEvent();

    public static void main(final String[] args) throws Exception {
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(32);
        final DirectBuffer directBuffer = new DirectBuffer(byteBuffer);
        final short messageTemplateVersion = 0;
        int bufferOffset = 0;

        // Setup for encoding a message

        MESSAGE_HEADER.wrap(directBuffer, bufferOffset, messageTemplateVersion)
                .blockLength(EVENT.sbeBlockLength())
                .templateId(EVENT.sbeTemplateId())
                .schemaId(EVENT.sbeSchemaId())
                .version(EVENT.sbeSchemaVersion());

        bufferOffset += MESSAGE_HEADER.size();

        // TODO
        final GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));
        final int highestExpectedRuntimeDurationAsNano = 10000;
        final Long[] durations = Lists.newArrayList(gf.limit(gf.incrementing(1l, 1l), highestExpectedRuntimeDurationAsNano)).toArray(new Long[highestExpectedRuntimeDurationAsNano]);
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
        final int benchmarkOperationCount = 1000000000;
        final long benchmarkStartTime = System.currentTimeMillis();
        for (int startTime = 0; startTime < benchmarkOperationCount; startTime++) {
            int operationTypeIndex = startTime % operationTypeCount;
            int operationType = operationTypes[operationTypeIndex];
            long duration = durations[startTime % highestExpectedRuntimeDurationAsNano];
            encode(EVENT, directBuffer, bufferOffset, (byte) 1, operationType, startTime, startTime, duration, resultCode);
            // baseline
        }
        final long benchmarkFinishTime = System.currentTimeMillis();
        final long benchmarkDurationMs = benchmarkFinishTime - benchmarkStartTime;

        final TemporalUtil temporalUtil = new TemporalUtil();
        final DecimalFormat decimalFormat = new DecimalFormat("###,###,###,###,###,##0");
        final double operationsPerMs = benchmarkOperationCount / (double) benchmarkDurationMs;
        final double operationsPerSecond = operationsPerMs * 1000;

        System.out.println(
                String.format(
                        "Completed %s events in %s --> %s op/s",
                        decimalFormat.format(benchmarkOperationCount),
                        temporalUtil.milliDurationToString(benchmarkDurationMs),
                        decimalFormat.format(operationsPerSecond)
                )
        );
        // TODO

        System.out.println("event.size = " + encode(EVENT, directBuffer, bufferOffset, (byte) 1, 2, 3, 4, 5, 6));

        // Decode the encoded message

        bufferOffset = 0;
        MESSAGE_HEADER.wrap(directBuffer, bufferOffset, messageTemplateVersion);

        // Lookup the applicable flyweight to decode this type of message based on templateId and version.
        final int templateId = MESSAGE_HEADER.templateId();
        if (templateId != MetricsEvent.TEMPLATE_ID) {
            throw new IllegalStateException("Template ids do not match");
        }

        final int actingBlockLength = MESSAGE_HEADER.blockLength();
        final int schemaId = MESSAGE_HEADER.schemaId();
        final int actingVersion = MESSAGE_HEADER.version();

        bufferOffset += MESSAGE_HEADER.size();
        decode(EVENT, directBuffer, bufferOffset, actingBlockLength, schemaId, actingVersion);
    }

    public static int encode(final MetricsEvent event, final DirectBuffer directBuffer, final int bufferOffset,
                             final byte eventType, final int operationType, final long scheduledStartTime, final long actualStartTime, final long runDuration, final int resultCode) {
        event.wrapForEncode(directBuffer, bufferOffset)
                .eventType(eventType)
                .operationType(operationType)
                .scheduledStartTimeAsMilli(scheduledStartTime)
                .actualStartTimeAsMilli(actualStartTime)
                .runDurationAsNano(runDuration)
                .resultCode(resultCode);
        return event.size();
    }

    public static void decode(
            final MetricsEvent event,
            final DirectBuffer directBuffer,
            final int bufferOffset,
            final int actingBlockLength,
            final int schemaId,
            final int actingVersion)
            throws Exception {
        final StringBuilder sb = new StringBuilder();

        event.wrapForDecode(directBuffer, bufferOffset, actingBlockLength, actingVersion);

        sb.append("\nevent.templateId=").append(event.sbeTemplateId());
        sb.append("\nevent.schemaId=").append(schemaId);
        sb.append("\nevent.schemaVersion=").append(event.sbeSchemaVersion());
        sb.append("\nevent.eventType=").append(event.eventType());
        sb.append("\nevent.operationType=").append(event.operationType());
        sb.append("\nevent.scheduledStartTimeAsMilli=").append(event.scheduledStartTimeAsMilli());
        sb.append("\nevent.actualStartTimeAsMilli=").append(event.actualStartTimeAsMilli());
        sb.append("\nevent.runDurationAsNano=").append(event.runDurationAsNano());
        sb.append("\nevent.resultCode=").append(event.resultCode());
        System.out.println(sb);
    }
}