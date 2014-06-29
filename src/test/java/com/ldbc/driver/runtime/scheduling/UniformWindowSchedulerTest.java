package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.*;
import com.ldbc.driver.generator.Window;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.testutils.NothingOperation;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class UniformWindowSchedulerTest {
    @Test
    public void shouldNotCrashWithEmptyWindow() throws OperationException {
        Time windowStartTimeInclusive = Time.fromMilli(0);
        Duration windowDuration = Duration.fromMilli(100);
        Window.OperationHandlerTimeRangeWindow window = new Window.OperationHandlerTimeRangeWindow(windowStartTimeInclusive, windowDuration);

        Scheduler<List<OperationHandler<?>>, Window.OperationHandlerTimeRangeWindow> scheduler = new UniformWindowedScheduler();

        List<OperationHandler<?>> handlers = scheduler.schedule(window);

        assertThat(handlers.size(), is(0));
    }

    @Test
    public void shouldUniformlySpreadStartTimesStartingFromBeginningOfWindowTimeFrame() throws OperationException {
        HandlerFactory handlerFactory = new HandlerFactory();
        Time windowStartTimeInclusive = Time.fromMilli(0);
        Duration windowDuration = Duration.fromMilli(100);
        Window.OperationHandlerTimeRangeWindow window = new Window.OperationHandlerTimeRangeWindow(windowStartTimeInclusive, windowDuration);

        window.add(handlerFactory.create(Time.fromMilli(10)));
        window.add(handlerFactory.create(Time.fromMilli(11)));
        window.add(handlerFactory.create(Time.fromMilli(12)));
        window.add(handlerFactory.create(Time.fromMilli(13)));

        Scheduler<List<OperationHandler<?>>, Window.OperationHandlerTimeRangeWindow> scheduler = new UniformWindowedScheduler();

        List<OperationHandler<?>> handlers = scheduler.schedule(window);

        assertThat(handlers.size(), is(4));
        assertThat(handlers.get(0).operation().scheduledStartTime(), is(Time.fromMilli(0)));
        assertThat(handlers.get(1).operation().scheduledStartTime(), is(Time.fromMilli(25)));
        assertThat(handlers.get(2).operation().scheduledStartTime(), is(Time.fromMilli(50)));
        assertThat(handlers.get(3).operation().scheduledStartTime(), is(Time.fromMilli(75)));
    }

    @Test
    public void shouldUniformlySpreadStartTimesStartingFromBeginningOfWindowTimeFrameWhenOriginalTimesAreNotInAscendingOrder() throws OperationException {
        HandlerFactory handlerFactory = new HandlerFactory();
        Time windowStartTimeInclusive = Time.fromMilli(0);
        Duration windowDuration = Duration.fromMilli(100);
        Window.OperationHandlerTimeRangeWindow window = new Window.OperationHandlerTimeRangeWindow(windowStartTimeInclusive, windowDuration);

        window.add(handlerFactory.create(Time.fromMilli(10)));
        window.add(handlerFactory.create(Time.fromMilli(8)));
        window.add(handlerFactory.create(Time.fromMilli(99)));
        window.add(handlerFactory.create(Time.fromMilli(4)));

        Scheduler<List<OperationHandler<?>>, Window.OperationHandlerTimeRangeWindow> scheduler = new UniformWindowedScheduler();

        List<OperationHandler<?>> handlers = scheduler.schedule(window);

        assertThat(handlers.size(), is(4));
        assertThat(handlers.get(0).operation().scheduledStartTime(), is(Time.fromMilli(0)));
        assertThat(handlers.get(1).operation().scheduledStartTime(), is(Time.fromMilli(25)));
        assertThat(handlers.get(2).operation().scheduledStartTime(), is(Time.fromMilli(50)));
        assertThat(handlers.get(3).operation().scheduledStartTime(), is(Time.fromMilli(75)));
    }

    @Test
    public void shouldStillUniformlySpreadStartTimesStartingFromBeginningOfWindowTimeFrameWhenTimesAreCloseTogether() throws OperationException {
        HandlerFactory handlerFactory = new HandlerFactory();
        Time windowStartTimeInclusive = Time.fromNano(0);
        Duration windowDuration = Duration.fromNano(3);
        Window.OperationHandlerTimeRangeWindow window = new Window.OperationHandlerTimeRangeWindow(windowStartTimeInclusive, windowDuration);

        window.add(handlerFactory.create(Time.fromNano(0)));
        window.add(handlerFactory.create(Time.fromNano(0)));
        window.add(handlerFactory.create(Time.fromNano(0)));
        window.add(handlerFactory.create(Time.fromNano(0)));
        window.add(handlerFactory.create(Time.fromNano(0)));
        window.add(handlerFactory.create(Time.fromNano(2)));

        Scheduler<List<OperationHandler<?>>, Window.OperationHandlerTimeRangeWindow> scheduler = new UniformWindowedScheduler();

        List<OperationHandler<?>> handlers = scheduler.schedule(window);

        assertThat(handlers.size(), is(6));
        assertThat(handlers.get(0).operation().scheduledStartTime(), is(Time.fromNano(0)));
        assertThat(handlers.get(1).operation().scheduledStartTime(), is(Time.fromNano(0)));
        assertThat(handlers.get(2).operation().scheduledStartTime(), is(Time.fromNano(1)));
        assertThat(handlers.get(3).operation().scheduledStartTime(), is(Time.fromNano(1)));
        assertThat(handlers.get(4).operation().scheduledStartTime(), is(Time.fromNano(2)));
        assertThat(handlers.get(5).operation().scheduledStartTime(), is(Time.fromNano(2)));
    }

    private static class HandlerFactory {
        OperationHandler<?> create(Time time) throws OperationException {
            OperationHandler<?> handler = new OperationHandler() {
                @Override
                protected OperationResultReport executeOperation(Operation operation) throws DbException {
                    return null;
                }
            };
            Operation operation = new NothingOperation();
            operation.setScheduledStartTime(time);
            handler.init(null, null, operation, null, null, null);
            return handler;
        }
    }
}
