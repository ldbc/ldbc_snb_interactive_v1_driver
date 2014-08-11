package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationException;
import com.ldbc.driver.generator.Window;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class UniformWindowOperationSchedulerTest {
    @Test
    public void shouldNotCrashWithEmptyWindow() throws OperationException {
        Time windowStartTimeInclusive = Time.fromMilli(0);
        Duration windowDuration = Duration.fromMilli(100);
        Window.OperationTimeRangeWindow window = new Window.OperationTimeRangeWindow(windowStartTimeInclusive, windowDuration);

        Scheduler<List<Operation<?>>, Window.OperationTimeRangeWindow> scheduler = new UniformWindowedOperationScheduler();

        List<Operation<?>> operations = scheduler.schedule(window);

        assertThat(operations.size(), is(0));
    }

    @Test
    public void shouldUniformlySpreadStartTimesStartingFromBeginningOfWindowTimeFrame() throws OperationException {
        Time windowStartTimeInclusive = Time.fromMilli(0);
        Duration windowDuration = Duration.fromMilli(100);
        Window.OperationTimeRangeWindow window = new Window.OperationTimeRangeWindow(windowStartTimeInclusive, windowDuration);

        window.add(new TimedNamedOperation1(Time.fromMilli(10), Time.fromMilli(0), null));
        window.add(new TimedNamedOperation1(Time.fromMilli(11), Time.fromMilli(0), null));
        window.add(new TimedNamedOperation1(Time.fromMilli(12), Time.fromMilli(0), null));
        window.add(new TimedNamedOperation1(Time.fromMilli(13), Time.fromMilli(0), null));

        Scheduler<List<Operation<?>>, Window.OperationTimeRangeWindow> scheduler = new UniformWindowedOperationScheduler();

        List<Operation<?>> operations = scheduler.schedule(window);

        assertThat(operations.size(), is(4));
        assertThat(operations.get(0).scheduledStartTime(), is(Time.fromMilli(0)));
        assertThat(operations.get(1).scheduledStartTime(), is(Time.fromMilli(25)));
        assertThat(operations.get(2).scheduledStartTime(), is(Time.fromMilli(50)));
        assertThat(operations.get(3).scheduledStartTime(), is(Time.fromMilli(75)));
    }

    @Test
    public void shouldUniformlySpreadStartTimesStartingFromBeginningOfWindowTimeFrameWhenOriginalTimesAreNotInAscendingOrder() throws OperationException {
        Time windowStartTimeInclusive = Time.fromMilli(0);
        Duration windowDuration = Duration.fromMilli(100);
        Window.OperationTimeRangeWindow window = new Window.OperationTimeRangeWindow(windowStartTimeInclusive, windowDuration);

        window.add(new TimedNamedOperation1(Time.fromMilli(10), Time.fromMilli(0), null));
        window.add(new TimedNamedOperation1(Time.fromMilli(8), Time.fromMilli(0), null));
        window.add(new TimedNamedOperation1(Time.fromMilli(99), Time.fromMilli(0), null));
        window.add(new TimedNamedOperation1(Time.fromMilli(4), Time.fromMilli(0), null));

        Scheduler<List<Operation<?>>, Window.OperationTimeRangeWindow> scheduler = new UniformWindowedOperationScheduler();

        List<Operation<?>> operations = scheduler.schedule(window);

        assertThat(operations.size(), is(4));
        assertThat(operations.get(0).scheduledStartTime(), is(Time.fromMilli(0)));
        assertThat(operations.get(1).scheduledStartTime(), is(Time.fromMilli(25)));
        assertThat(operations.get(2).scheduledStartTime(), is(Time.fromMilli(50)));
        assertThat(operations.get(3).scheduledStartTime(), is(Time.fromMilli(75)));
    }

    @Test
    public void shouldStillUniformlySpreadStartTimesStartingFromBeginningOfWindowTimeFrameWhenTimesAreCloseTogether() throws OperationException {
        Time windowStartTimeInclusive = Time.fromNano(0);
        Duration windowDuration = Duration.fromNano(3);
        Window.OperationTimeRangeWindow window = new Window.OperationTimeRangeWindow(windowStartTimeInclusive, windowDuration);

        window.add(new TimedNamedOperation1(Time.fromNano(0), Time.fromNano(0), null));
        window.add(new TimedNamedOperation1(Time.fromNano(0), Time.fromNano(0), null));
        window.add(new TimedNamedOperation1(Time.fromNano(0), Time.fromNano(0), null));
        window.add(new TimedNamedOperation1(Time.fromNano(0), Time.fromNano(0), null));
        window.add(new TimedNamedOperation1(Time.fromNano(0), Time.fromNano(0), null));
        window.add(new TimedNamedOperation1(Time.fromNano(2), Time.fromNano(0), null));

        Scheduler<List<Operation<?>>, Window.OperationTimeRangeWindow> scheduler = new UniformWindowedOperationScheduler();

        List<Operation<?>> operations = scheduler.schedule(window);

        assertThat(operations.size(), is(6));
        assertThat(operations.get(0).scheduledStartTime(), is(Time.fromNano(0)));
        assertThat(operations.get(1).scheduledStartTime(), is(Time.fromNano(0)));
        assertThat(operations.get(2).scheduledStartTime(), is(Time.fromNano(1)));
        assertThat(operations.get(3).scheduledStartTime(), is(Time.fromNano(1)));
        assertThat(operations.get(4).scheduledStartTime(), is(Time.fromNano(2)));
        assertThat(operations.get(5).scheduledStartTime(), is(Time.fromNano(2)));
    }
}
