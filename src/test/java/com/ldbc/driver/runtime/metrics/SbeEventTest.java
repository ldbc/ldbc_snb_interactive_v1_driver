package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.runtime.metrics.sbe.MetricsEvent;
import org.junit.Test;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SbeEventTest {
    @Test
    public void shouldReadWrites() throws WorkloadException, MetricsCollectionException {
        final DirectBuffer directBuffer = new DisruptorSbeMetricsEvent.MetricsCollectionEventFactory().newInstance();
        final MetricsEvent metricsEvent = new MetricsEvent();

        byte eventType = DisruptorSbeMetricsEvent.SUBMIT_OPERATION_RESULT;
        int operationType = Integer.MAX_VALUE;
        long scheduledStartTime = Long.MAX_VALUE;
        long actualStartTime = Long.MAX_VALUE;
        long runDuration = Long.MAX_VALUE;
        int resultCode = Integer.MAX_VALUE;

        metricsEvent.wrapForEncode(directBuffer, DisruptorSbeMetricsEvent.MESSAGE_HEADER_SIZE);
        metricsEvent.eventType(eventType);
        metricsEvent.operationType(operationType);
        metricsEvent.scheduledStartTimeAsMilli(scheduledStartTime);
        metricsEvent.actualStartTimeAsMilli(actualStartTime);
        metricsEvent.runDurationAsNano(runDuration);
        metricsEvent.resultCode(resultCode);

        metricsEvent.wrapForDecode(
                directBuffer,
                DisruptorSbeMetricsEvent.MESSAGE_HEADER_SIZE,
                DisruptorSbeMetricsEvent.ACTING_BLOCK_LENGTH,
                DisruptorSbeMetricsEvent.ACTING_VERSION);
        assertThat(metricsEvent.eventType(), is(eventType));
        assertThat(metricsEvent.scheduledStartTimeAsMilli(), is(scheduledStartTime));
        assertThat(metricsEvent.actualStartTimeAsMilli(), is(actualStartTime));
        assertThat(metricsEvent.runDurationAsNano(), is(runDuration));
        assertThat(metricsEvent.resultCode(), is(resultCode));

        eventType = DisruptorSbeMetricsEvent.SUBMIT_OPERATION_RESULT;
        scheduledStartTime = 1;
        actualStartTime = 2;
        runDuration = 3;
        resultCode = 4;
        metricsEvent.wrapForEncode(directBuffer, DisruptorSbeMetricsEvent.MESSAGE_HEADER_SIZE)
                .eventType(eventType)
                .scheduledStartTimeAsMilli(scheduledStartTime)
                .actualStartTimeAsMilli(actualStartTime)
                .runDurationAsNano(runDuration)
                .resultCode(resultCode);
        metricsEvent.wrapForDecode(
                directBuffer,
                DisruptorSbeMetricsEvent.MESSAGE_HEADER_SIZE,
                DisruptorSbeMetricsEvent.ACTING_BLOCK_LENGTH,
                DisruptorSbeMetricsEvent.ACTING_VERSION);
        assertThat(metricsEvent.eventType(), is(eventType));
        assertThat(metricsEvent.scheduledStartTimeAsMilli(), is(scheduledStartTime));
        assertThat(metricsEvent.actualStartTimeAsMilli(), is(actualStartTime));
        assertThat(metricsEvent.runDurationAsNano(), is(runDuration));
        assertThat(metricsEvent.resultCode(), is(resultCode));

        metricsEvent.wrapForDecode(
                directBuffer,
                DisruptorSbeMetricsEvent.MESSAGE_HEADER_SIZE,
                DisruptorSbeMetricsEvent.ACTING_BLOCK_LENGTH,
                DisruptorSbeMetricsEvent.ACTING_VERSION);
        assertThat(metricsEvent.eventType(), is(eventType));
        assertThat(metricsEvent.scheduledStartTimeAsMilli(), is(scheduledStartTime));
        assertThat(metricsEvent.actualStartTimeAsMilli(), is(actualStartTime));
        assertThat(metricsEvent.runDurationAsNano(), is(runDuration));
        assertThat(metricsEvent.resultCode(), is(resultCode));
    }
}
