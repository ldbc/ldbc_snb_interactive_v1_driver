package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.runtime.metrics.sbe.MessageHeader;
import com.ldbc.driver.runtime.metrics.sbe.MetricsEvent;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.EventTranslatorVararg;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.nio.ByteBuffer;

public class DisruptorMetricsEvent_NEW {
    private static final MetricsEvent METRICS_EVENT = new MetricsEvent();
    private static final MessageHeader MESSAGE_HEADER = new MessageHeader();
    private static final short MESSAGE_TEMPLATE_VERSION = 0;

    public static final int MESSAGE_HEADER_SIZE;
    public static final int ACTING_BLOCK_LENGTH;
    public static final int SCHEMA_ID;
    public static final int ACTING_VERSION;

    public static class MetricsCollectionEventFactory implements EventFactory<DirectBuffer> {
        @Override
        public DirectBuffer newInstance() {
            final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(32);
            DirectBuffer directBuffer = new DirectBuffer(byteBuffer);
            MESSAGE_HEADER.wrap(directBuffer, 0, MESSAGE_TEMPLATE_VERSION)
                    .blockLength(METRICS_EVENT.sbeBlockLength())
                    .templateId(METRICS_EVENT.sbeTemplateId())
                    .schemaId(METRICS_EVENT.sbeSchemaId())
                    .version(METRICS_EVENT.sbeSchemaVersion());
            return directBuffer;
        }
    }

    static {
        DirectBuffer buffer = new DisruptorMetricsEvent_NEW.MetricsCollectionEventFactory().newInstance();
        MESSAGE_HEADER.wrap(buffer, 0, MESSAGE_TEMPLATE_VERSION)
                .blockLength(METRICS_EVENT.sbeBlockLength())
                .templateId(METRICS_EVENT.sbeTemplateId())
                .schemaId(METRICS_EVENT.sbeSchemaId())
                .version(METRICS_EVENT.sbeSchemaVersion());
        MESSAGE_HEADER_SIZE = MESSAGE_HEADER.size();
        ACTING_BLOCK_LENGTH = MESSAGE_HEADER.blockLength();
        SCHEMA_ID = MESSAGE_HEADER.schemaId();
        ACTING_VERSION = MESSAGE_HEADER.version();
    }

    // Submit operation result for its metrics to be collected
    public static final byte SUBMIT_RESULT = 1;
    // Request metrics summary
    public static final byte WORKLOAD_STATUS = 2;
    // Request complete workload results
    public static final byte WORKLOAD_RESULT = 3;

    public static String toString(DirectBuffer event) {
        METRICS_EVENT.wrapForDecode(event, MESSAGE_HEADER_SIZE, ACTING_BLOCK_LENGTH, ACTING_VERSION);
        return "METRICS_EVENT.eventType(){" +
                "eventType=" + METRICS_EVENT.eventType() +
                ", operationType=" + METRICS_EVENT.operationType() +
                ", scheduledStartTimeAsMilli=" + METRICS_EVENT.scheduledStartTimeAsMilli() +
                ", actualStartTimeAsMilli=" + METRICS_EVENT.actualStartTimeAsMilli() +
                ", runDurationAsNano=" + METRICS_EVENT.runDurationAsNano() +
                ", resultCode=" + METRICS_EVENT.resultCode() +
                '}';
    }

    public static final EventTranslatorVararg<DirectBuffer> SET_AS_SUBMIT_OPERATION_RESULT =
            new EventTranslatorVararg<DirectBuffer>() {
                @Override
                public void translateTo(DirectBuffer event, long l, Object... fields) {
                    METRICS_EVENT.wrapForEncode(event, MESSAGE_HEADER_SIZE)
                            .eventType(SUBMIT_RESULT)
                            .operationType((int) fields[0])
                            .scheduledStartTimeAsMilli((long) fields[1])
                            .actualStartTimeAsMilli((long) fields[2])
                            .runDurationAsNano((long) fields[3])
                            .resultCode((int) fields[4]);
                }
            };

    public static final EventTranslator<DirectBuffer> SET_AS_STATUS =
            new EventTranslator<DirectBuffer>() {
                @Override
                public void translateTo(DirectBuffer event, long l) {
                    METRICS_EVENT.wrapForEncode(event, MESSAGE_HEADER_SIZE)
                            .eventType(WORKLOAD_STATUS);
                }
            };

    public static final EventTranslator<DirectBuffer> SET_AS_REQUEST_WORKLOAD_RESULT =
            new EventTranslator<DirectBuffer>() {
                @Override
                public void translateTo(DirectBuffer event, long l) {
                    METRICS_EVENT.wrapForEncode(event, MESSAGE_HEADER_SIZE)
                            .eventType(WORKLOAD_RESULT);
                }
            };
}