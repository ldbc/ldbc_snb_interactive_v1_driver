package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.Operation;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.EventTranslatorVararg;
import javolution.io.Struct;

public class DisruptorMetricsCollectionEvent extends Struct {
    public static class MetricsCollectionEventFactory implements EventFactory<DisruptorMetricsCollectionEvent> {
        @Override
        public DisruptorMetricsCollectionEvent newInstance() {
            return new DisruptorMetricsCollectionEvent();
        }
    }

    // Submit operation result for its metrics to be collected
    public static final byte SUBMIT_RESULT = 1;
    // Request metrics summary
    public static final byte WORKLOAD_STATUS = 2;
    // Request complete workload results
    public static final byte WORKLOAD_RESULT = 3;

    private final Signed8 eventType;
    private final Signed32 operationType;
    private final Signed64 scheduledStartTimeAsMilli;
    private final Signed64 actualStartTimeAsMilli;
    private final Signed64 runDurationAsNano;
    private final Signed32 resultCode;

    public DisruptorMetricsCollectionEvent() {
        this.eventType = new Signed8();
        this.operationType = new Signed32();
        this.scheduledStartTimeAsMilli = new Signed64();
        this.actualStartTimeAsMilli = new Signed64();
        this.runDurationAsNano = new Signed64();
        this.resultCode = new Signed32();
    }

    @Override
    public boolean isPacked() {
        return true;
    }

    public byte eventType() {
        return eventType.get();
    }

    public int operationType() {
        return operationType.get();
    }

    public long scheduledStartTimeAsMilli() {
        return scheduledStartTimeAsMilli.get();
    }

    public long actualStartTimeAsMilli() {
        return actualStartTimeAsMilli.get();
    }

    public long runDurationAsNano() {
        return runDurationAsNano.get();
    }

    public int resultCode() {
        return resultCode.get();
    }

    public void setEventType(byte eventType) {
        this.eventType.set(eventType);
    }

    public void setOperationType(int operationType) {
        this.operationType.set(operationType);
    }

    public void setScheduledStartTimeAsMilli(long scheduledStartTimeAsMilli) {
        this.scheduledStartTimeAsMilli.set(scheduledStartTimeAsMilli);
    }

    public void setActualStartTimeAsMilli(long actualStartTimeAsMilli) {
        this.actualStartTimeAsMilli.set(actualStartTimeAsMilli);
    }

    public void setRunDurationAsNano(long runDurationAsNano) {
        this.runDurationAsNano.set(runDurationAsNano);
    }

    public void setResultCode(int resultCode) {
        this.resultCode.set(resultCode);
    }

    @Override
    public String toString() {
        return "DisruptorMetricsCollectionEvent{" +
                "eventType=" + eventType +
                ", operationType=" + operationType +
                ", scheduledStartTimeAsMilli=" + scheduledStartTimeAsMilli +
                ", actualStartTimeAsMilli=" + actualStartTimeAsMilli +
                ", runDurationAsNano=" + runDurationAsNano +
                ", resultCode=" + resultCode +
                '}';
    }

    public static final EventTranslatorVararg<DisruptorMetricsCollectionEvent> SET_AS_SUBMIT_OPERATION_RESULT =
            new EventTranslatorVararg<DisruptorMetricsCollectionEvent>() {
                @Override
                public void translateTo(DisruptorMetricsCollectionEvent event, long l, Object... fields) {
                    event.setEventType(SUBMIT_RESULT);
                    event.setOperationType((int) fields[0]);
                    event.setScheduledStartTimeAsMilli((long) fields[1]);
                    event.setActualStartTimeAsMilli((long) fields[2]);
                    event.setRunDurationAsNano((long) fields[3]);
                    event.setResultCode((int) fields[4]);
                }
            };

    public static final EventTranslator<DisruptorMetricsCollectionEvent> SET_AS_STATUS =
            new EventTranslator<DisruptorMetricsCollectionEvent>() {
                @Override
                public void translateTo(DisruptorMetricsCollectionEvent event, long l) {
                    event.setEventType(WORKLOAD_STATUS);
                }
            };

    public static final EventTranslator<DisruptorMetricsCollectionEvent> SET_AS_REQUEST_WORKLOAD_RESULT =
            new EventTranslator<DisruptorMetricsCollectionEvent>() {
                @Override
                public void translateTo(DisruptorMetricsCollectionEvent event, long l) {
                    event.setEventType(WORKLOAD_RESULT);
                }
            };
}