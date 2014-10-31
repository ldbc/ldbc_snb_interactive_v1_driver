package com.ldbc.driver;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Queues;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;
import org.junit.Test;

import java.text.DecimalFormat;
import java.util.Queue;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class BoundedBufferTest {
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
    private static final TimeSource TIME_SOURCE = new SystemTimeSource();
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("###,###,###,##0.00");
    private static final DecimalFormat INTEGRAL_FORMAT = new DecimalFormat("###,###,###,###");

    @Test
    public void evictingQueuePerformanceTest() {
        int count = 100000000;
        int bound = 10000;
        EvictingQueue<Integer> queue = EvictingQueue.create(bound);

        long nanoStart = TIME_SOURCE.nanoSnapshot();
        doBoundedNonBlockingQueueAddPerformanceTest(queue, count);
        long nanoStop = TIME_SOURCE.nanoSnapshot();

        for (int i = bound; i > 0; i--) {
            assertThat(queue.poll(), is(count - i));
        }

        System.out.println(
                String.format("%s added %s things in %s = %s things/ms",
                        queue.getClass().getSimpleName(),
                        INTEGRAL_FORMAT.format(count),
                        TEMPORAL_UTIL.nanoDurationToString(nanoStop - nanoStart),
                        DECIMAL_FORMAT.format((count / (double) (nanoStop - nanoStart) * 1000000))
                )
        );

        nanoStart = TIME_SOURCE.nanoSnapshot();
        doBoundedNonBlockingQueueAddPerformanceTest(Queues.synchronizedQueue(queue), count);
        nanoStop = TIME_SOURCE.nanoSnapshot();

        for (int i = bound; i > 0; i--) {
            assertThat(queue.poll(), is(count - i));
        }

        System.out.println(
                String.format("Synchronized %s added %s things in %s = %s things/ms",
                        queue.getClass().getSimpleName(),
                        INTEGRAL_FORMAT.format(count),
                        TEMPORAL_UTIL.nanoDurationToString(nanoStop - nanoStart),
                        DECIMAL_FORMAT.format((count / (double) (nanoStop - nanoStart) * 1000000))
                )
        );
    }

    @Test
    public void evictingQueueAddRemovePerformanceTest() {
        int count = 100000000;
        int bound = 10000;
        EvictingQueue<Integer> queue = EvictingQueue.create(bound);

        long nanoStart = TIME_SOURCE.nanoSnapshot();
        doBoundedNonBlockingQueueAddRemovePerformanceTest(queue, count);
        long nanoStop = TIME_SOURCE.nanoSnapshot();

        System.out.println(
                String.format("%s added %s things in %s = %s things/ms",
                        queue.getClass().getSimpleName(),
                        INTEGRAL_FORMAT.format(count),
                        TEMPORAL_UTIL.nanoDurationToString(nanoStop - nanoStart),
                        DECIMAL_FORMAT.format((count / (double) (nanoStop - nanoStart) * 1000000))
                )
        );

        nanoStart = TIME_SOURCE.nanoSnapshot();
        doBoundedNonBlockingQueueAddRemovePerformanceTest(Queues.synchronizedQueue(queue), count);
        nanoStop = TIME_SOURCE.nanoSnapshot();

        System.out.println(
                String.format("Synchronized %s added %s things in %s = %s things/ms",
                        queue.getClass().getSimpleName(),
                        INTEGRAL_FORMAT.format(count),
                        TEMPORAL_UTIL.nanoDurationToString(nanoStop - nanoStart),
                        DECIMAL_FORMAT.format((count / (double) (nanoStop - nanoStart) * 1000000))
                )
        );
    }

    public void doBoundedNonBlockingQueueAddPerformanceTest(Queue<Integer> queue, int count) {
        for (int i = 0; i < count; i++) {
            queue.add(i);
        }
    }

    public void doBoundedNonBlockingQueueAddRemovePerformanceTest(Queue<Integer> queue, int count) {
        for (int i = 0; i < count; i++) {
            queue.add(i);
            queue.poll();
        }
    }

    @Test
    public void evictingQueueShouldWorkAsExpected() {
        EvictingQueue<Integer> evictingQueue = EvictingQueue.create(3);

        doBoundedNonBlockingQueueShouldWorkAsExpected(evictingQueue);
    }

    public void doBoundedNonBlockingQueueShouldWorkAsExpected(Queue<Integer> queue) {
        queue.add(1);

        assertThat(queue.size(), is(1));
        assertThat(queue.poll(), is(1));
        assertThat(queue.size(), is(0));

        queue.add(1);
        queue.add(2);
        queue.add(3);

        assertThat(queue.size(), is(3));

        assertThat(queue.poll(), is(1));
        assertThat(queue.size(), is(2));

        assertThat(queue.poll(), is(2));
        assertThat(queue.size(), is(1));

        assertThat(queue.poll(), is(3));
        assertThat(queue.size(), is(0));

        queue.add(1);
        queue.add(2);
        queue.add(3);
        queue.add(4);

        assertThat(queue.size(), is(3));

        assertThat(queue.poll(), is(2));
        assertThat(queue.size(), is(2));

        assertThat(queue.poll(), is(3));
        assertThat(queue.size(), is(1));

        assertThat(queue.poll(), is(4));
        assertThat(queue.size(), is(0));
    }
}
