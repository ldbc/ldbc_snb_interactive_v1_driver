package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationException;
import com.ldbc.driver.generator.Window;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class UniformWindowOperationSchedulerTest {
    @Test
    public void shouldNotCrashWithEmptyWindow() throws OperationException {
        long windowStartTimeInclusive = 0l;
        long windowDuration = 100l;
        Window.OperationTimeRangeWindow window = new Window.OperationTimeRangeWindow(windowStartTimeInclusive, windowDuration);

        Scheduler<List<Operation<?>>, Window.OperationTimeRangeWindow> scheduler = new UniformWindowedOperationScheduler();

        List<Operation<?>> operations = scheduler.schedule(window);

        assertThat(operations.size(), is(0));
    }

    @Test
    public void shouldUniformlySpreadStartTimesStartingFromBeginningOfWindowTimeFrame() throws OperationException {
        long windowStartTimeInclusive = 0l;
        long windowDuration = 100l;
        Window.OperationTimeRangeWindow window = new Window.OperationTimeRangeWindow(windowStartTimeInclusive, windowDuration);

        window.add(new TimedNamedOperation1(10l, 0l, null));
        window.add(new TimedNamedOperation1(11l, 0l, null));
        window.add(new TimedNamedOperation1(12l, 0l, null));
        window.add(new TimedNamedOperation1(13l, 0l, null));

        Scheduler<List<Operation<?>>, Window.OperationTimeRangeWindow> scheduler = new UniformWindowedOperationScheduler();

        List<Operation<?>> operations = scheduler.schedule(window);

        assertThat(operations.size(), is(4));
        assertThat(operations.get(0).scheduledStartTimeAsMilli(), is(0l));
        assertThat(operations.get(1).scheduledStartTimeAsMilli(), is(25l));
        assertThat(operations.get(2).scheduledStartTimeAsMilli(), is(50l));
        assertThat(operations.get(3).scheduledStartTimeAsMilli(), is(75l));
    }

    @Test
    public void shouldUniformlySpreadStartTimesStartingFromBeginningOfWindowTimeFrameWhenOriginalTimesAreNotInAscendingOrder() throws OperationException {
        long windowStartTimeInclusive = 0l;
        long windowDuration = 100l;
        Window.OperationTimeRangeWindow window = new Window.OperationTimeRangeWindow(windowStartTimeInclusive, windowDuration);

        window.add(new TimedNamedOperation1(10l, 0l, null));
        window.add(new TimedNamedOperation1(8l, 0l, null));
        window.add(new TimedNamedOperation1(99l, 0l, null));
        window.add(new TimedNamedOperation1(4l, 0l, null));

        Scheduler<List<Operation<?>>, Window.OperationTimeRangeWindow> scheduler = new UniformWindowedOperationScheduler();

        List<Operation<?>> operations = scheduler.schedule(window);

        assertThat(operations.size(), is(4));
        assertThat(operations.get(0).scheduledStartTimeAsMilli(), is(0l));
        assertThat(operations.get(1).scheduledStartTimeAsMilli(), is(25l));
        assertThat(operations.get(2).scheduledStartTimeAsMilli(), is(50l));
        assertThat(operations.get(3).scheduledStartTimeAsMilli(), is(75l));
    }

    @Test
    public void shouldStillUniformlySpreadStartTimesStartingFromBeginningOfWindowTimeFrameWhenTimesAreCloseTogether() throws OperationException {
        long windowStartTimeInclusive = 0l;
        long windowDuration = 3l;
        Window.OperationTimeRangeWindow window = new Window.OperationTimeRangeWindow(windowStartTimeInclusive, windowDuration);

        window.add(new TimedNamedOperation1(0l, 0l, null));
        window.add(new TimedNamedOperation1(0l, 0l, null));
        window.add(new TimedNamedOperation1(0l, 0l, null));
        window.add(new TimedNamedOperation1(0l, 0l, null));
        window.add(new TimedNamedOperation1(0l, 0l, null));
        window.add(new TimedNamedOperation1(2l, 0l, null));

        Scheduler<List<Operation<?>>, Window.OperationTimeRangeWindow> scheduler = new UniformWindowedOperationScheduler();

        List<Operation<?>> operations = scheduler.schedule(window);

        assertThat(operations.size(), is(6));
        assertThat(operations.get(0).scheduledStartTimeAsMilli(), is(0l));
        assertThat(operations.get(1).scheduledStartTimeAsMilli(), is(0l));
        assertThat(operations.get(2).scheduledStartTimeAsMilli(), is(1l));
        assertThat(operations.get(3).scheduledStartTimeAsMilli(), is(1l));
        assertThat(operations.get(4).scheduledStartTimeAsMilli(), is(2l));
        assertThat(operations.get(5).scheduledStartTimeAsMilli(), is(2l));
    }
}
