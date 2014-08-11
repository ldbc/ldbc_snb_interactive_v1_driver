package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.*;
import com.ldbc.driver.generator.Window;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.workloads.dummy.DummyDb;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class UniformWindowOperationHandlerSchedulerTest {
    @Test
    public void shouldNotCrashWithEmptyWindow() throws OperationException {
        Time windowStartTimeInclusive = Time.fromMilli(0);
        Duration windowDuration = Duration.fromMilli(100);
        Window.OperationHandlerTimeRangeWindow window = new Window.OperationHandlerTimeRangeWindow(windowStartTimeInclusive, windowDuration);

        Scheduler<List<OperationHandler<?>>, Window.OperationHandlerTimeRangeWindow> scheduler = new UniformWindowedOperationHandlerScheduler();

        List<OperationHandler<?>> handlers = scheduler.schedule(window);

        assertThat(handlers.size(), is(0));
    }

    @Test
    public void shouldUniformlySpreadStartTimesStartingFromBeginningOfWindowTimeFrame() throws OperationException, DbException {
        Time windowStartTimeInclusive = Time.fromMilli(0);
        Duration windowDuration = Duration.fromMilli(100);
        Window.OperationHandlerTimeRangeWindow window = new Window.OperationHandlerTimeRangeWindow(windowStartTimeInclusive, windowDuration);

        Db db = new DummyDb();
        db.init(new HashMap<String, String>());
        OperationHandler<?> operationHandler1 = null;
        OperationHandler<?> operationHandler2 = null;
        OperationHandler<?> operationHandler3 = null;
        OperationHandler<?> operationHandler4 = null;
        try {
            Operation<?> operation1 = new TimedNamedOperation1(Time.fromMilli(10), Time.fromNano(0), "name");
            Operation<?> operation2 = new TimedNamedOperation1(Time.fromMilli(11), Time.fromNano(0), "name");
            Operation<?> operation3 = new TimedNamedOperation1(Time.fromMilli(12), Time.fromNano(0), "name");
            Operation<?> operation4 = new TimedNamedOperation1(Time.fromMilli(13), Time.fromNano(0), "name");

            operationHandler1 = db.getOperationHandler(operation1);
            operationHandler2 = db.getOperationHandler(operation2);
            operationHandler3 = db.getOperationHandler(operation3);
            operationHandler4 = db.getOperationHandler(operation4);

            operationHandler1.init(null, null, operation1, null, null, null);
            operationHandler2.init(null, null, operation2, null, null, null);
            operationHandler3.init(null, null, operation3, null, null, null);
            operationHandler4.init(null, null, operation4, null, null, null);

            window.add(operationHandler1);
            window.add(operationHandler2);
            window.add(operationHandler3);
            window.add(operationHandler4);

            Scheduler<List<OperationHandler<?>>, Window.OperationHandlerTimeRangeWindow> scheduler = new UniformWindowedOperationHandlerScheduler();

            List<OperationHandler<?>> handlers = scheduler.schedule(window);

            assertThat(handlers.size(), is(4));
            assertThat(handlers.get(0).operation().scheduledStartTime(), is(Time.fromMilli(0)));
            assertThat(handlers.get(1).operation().scheduledStartTime(), is(Time.fromMilli(25)));
            assertThat(handlers.get(2).operation().scheduledStartTime(), is(Time.fromMilli(50)));
            assertThat(handlers.get(3).operation().scheduledStartTime(), is(Time.fromMilli(75)));
        } finally {
            if (null != operationHandler1) operationHandler1.cleanup();
            if (null != operationHandler2) operationHandler2.cleanup();
            if (null != operationHandler3) operationHandler3.cleanup();
            if (null != operationHandler4) operationHandler4.cleanup();
            if (null != db) db.cleanup();
        }
    }

    @Test
    public void shouldUniformlySpreadStartTimesStartingFromBeginningOfWindowTimeFrameWhenOriginalTimesAreNotInAscendingOrder() throws OperationException, DbException {
        Time windowStartTimeInclusive = Time.fromMilli(0);
        Duration windowDuration = Duration.fromMilli(100);
        Window.OperationHandlerTimeRangeWindow window = new Window.OperationHandlerTimeRangeWindow(windowStartTimeInclusive, windowDuration);

        Db db = new DummyDb();
        db.init(new HashMap<String, String>());
        OperationHandler<?> operationHandler1 = null;
        OperationHandler<?> operationHandler2 = null;
        OperationHandler<?> operationHandler3 = null;
        OperationHandler<?> operationHandler4 = null;
        try {
            Operation<?> operation1 = new TimedNamedOperation1(Time.fromMilli(10), Time.fromNano(0), "name");
            Operation<?> operation2 = new TimedNamedOperation1(Time.fromMilli(8), Time.fromNano(0), "name");
            Operation<?> operation3 = new TimedNamedOperation1(Time.fromMilli(99), Time.fromNano(0), "name");
            Operation<?> operation4 = new TimedNamedOperation1(Time.fromMilli(4), Time.fromNano(0), "name");

            operationHandler1 = db.getOperationHandler(operation1);
            operationHandler2 = db.getOperationHandler(operation2);
            operationHandler3 = db.getOperationHandler(operation3);
            operationHandler4 = db.getOperationHandler(operation4);

            operationHandler1.init(null, null, operation1, null, null, null);
            operationHandler2.init(null, null, operation2, null, null, null);
            operationHandler3.init(null, null, operation3, null, null, null);
            operationHandler4.init(null, null, operation4, null, null, null);

            window.add(operationHandler1);
            window.add(operationHandler2);
            window.add(operationHandler3);
            window.add(operationHandler4);

            Scheduler<List<OperationHandler<?>>, Window.OperationHandlerTimeRangeWindow> scheduler = new UniformWindowedOperationHandlerScheduler();

            List<OperationHandler<?>> handlers = scheduler.schedule(window);

            assertThat(handlers.size(), is(4));
            assertThat(handlers.get(0).operation().scheduledStartTime(), is(Time.fromMilli(0)));
            assertThat(handlers.get(1).operation().scheduledStartTime(), is(Time.fromMilli(25)));
            assertThat(handlers.get(2).operation().scheduledStartTime(), is(Time.fromMilli(50)));
            assertThat(handlers.get(3).operation().scheduledStartTime(), is(Time.fromMilli(75)));
        } finally {
            if (null != operationHandler1) operationHandler1.cleanup();
            if (null != operationHandler2) operationHandler2.cleanup();
            if (null != operationHandler3) operationHandler3.cleanup();
            if (null != operationHandler4) operationHandler4.cleanup();
            if (null != db) db.cleanup();
        }
    }

    @Test
    public void shouldStillUniformlySpreadStartTimesStartingFromBeginningOfWindowTimeFrameWhenTimesAreCloseTogether() throws OperationException, DbException {
        Time windowStartTimeInclusive = Time.fromNano(0);
        Duration windowDuration = Duration.fromNano(3);
        Window.OperationHandlerTimeRangeWindow window = new Window.OperationHandlerTimeRangeWindow(windowStartTimeInclusive, windowDuration);

        Db db = new DummyDb();
        db.init(new HashMap<String, String>());
        OperationHandler<?> operationHandler1 = null;
        OperationHandler<?> operationHandler2 = null;
        OperationHandler<?> operationHandler3 = null;
        OperationHandler<?> operationHandler4 = null;
        OperationHandler<?> operationHandler5 = null;
        OperationHandler<?> operationHandler6 = null;
        try {
            Operation<?> operation1 = new TimedNamedOperation1(Time.fromNano(0), Time.fromNano(0), "name");
            Operation<?> operation2 = new TimedNamedOperation1(Time.fromNano(0), Time.fromNano(0), "name");
            Operation<?> operation3 = new TimedNamedOperation1(Time.fromNano(0), Time.fromNano(0), "name");
            Operation<?> operation4 = new TimedNamedOperation1(Time.fromNano(0), Time.fromNano(0), "name");
            Operation<?> operation5 = new TimedNamedOperation1(Time.fromNano(0), Time.fromNano(0), "name");
            Operation<?> operation6 = new TimedNamedOperation1(Time.fromNano(2), Time.fromNano(0), "name");

            operationHandler1 = db.getOperationHandler(operation1);
            operationHandler2 = db.getOperationHandler(operation2);
            operationHandler3 = db.getOperationHandler(operation3);
            operationHandler4 = db.getOperationHandler(operation4);
            operationHandler5 = db.getOperationHandler(operation5);
            operationHandler6 = db.getOperationHandler(operation6);

            operationHandler1.init(null, null, operation1, null, null, null);
            operationHandler2.init(null, null, operation2, null, null, null);
            operationHandler3.init(null, null, operation3, null, null, null);
            operationHandler4.init(null, null, operation4, null, null, null);
            operationHandler5.init(null, null, operation5, null, null, null);
            operationHandler6.init(null, null, operation6, null, null, null);

            window.add(operationHandler1);
            window.add(operationHandler2);
            window.add(operationHandler3);
            window.add(operationHandler4);
            window.add(operationHandler5);
            window.add(operationHandler6);

            Scheduler<List<OperationHandler<?>>, Window.OperationHandlerTimeRangeWindow> scheduler = new UniformWindowedOperationHandlerScheduler();

            List<OperationHandler<?>> handlers = scheduler.schedule(window);

            assertThat(handlers.size(), is(6));
            assertThat(handlers.get(0).operation().scheduledStartTime(), is(Time.fromNano(0)));
            assertThat(handlers.get(1).operation().scheduledStartTime(), is(Time.fromNano(0)));
            assertThat(handlers.get(2).operation().scheduledStartTime(), is(Time.fromNano(1)));
            assertThat(handlers.get(3).operation().scheduledStartTime(), is(Time.fromNano(1)));
            assertThat(handlers.get(4).operation().scheduledStartTime(), is(Time.fromNano(2)));
            assertThat(handlers.get(5).operation().scheduledStartTime(), is(Time.fromNano(2)));
        } finally {
            if (null != operationHandler1) operationHandler1.cleanup();
            if (null != operationHandler2) operationHandler2.cleanup();
            if (null != operationHandler3) operationHandler3.cleanup();
            if (null != operationHandler4) operationHandler4.cleanup();
            if (null != operationHandler5) operationHandler5.cleanup();
            if (null != operationHandler6) operationHandler6.cleanup();
            if (null != db) db.cleanup();
        }
    }

//    @Test
//    public void shouldUniformlySpreadStartTimesStartingFromBeginningOfWindowTimeFrame() throws OperationException {
//        HandlerFactory handlerFactory = new HandlerFactory();
//        Time windowStartTimeInclusive = Time.fromMilli(0);
//        Duration windowDuration = Duration.fromMilli(100);
//        Window.OperationHandlerTimeRangeWindow window = new Window.OperationHandlerTimeRangeWindow(windowStartTimeInclusive, windowDuration);
//
//        window.add(handlerFactory.create(Time.fromMilli(10)));
//        window.add(handlerFactory.create(Time.fromMilli(11)));
//        window.add(handlerFactory.create(Time.fromMilli(12)));
//        window.add(handlerFactory.create(Time.fromMilli(13)));
//
//        Scheduler<List<OperationHandler<?>>, Window.OperationHandlerTimeRangeWindow> scheduler = new UniformWindowedOperationHandlerScheduler();
//
//        List<OperationHandler<?>> handlers = scheduler.schedule(window);
//
//        assertThat(handlers.size(), is(4));
//        assertThat(handlers.get(0).operation().scheduledStartTime(), is(Time.fromMilli(0)));
//        assertThat(handlers.get(1).operation().scheduledStartTime(), is(Time.fromMilli(25)));
//        assertThat(handlers.get(2).operation().scheduledStartTime(), is(Time.fromMilli(50)));
//        assertThat(handlers.get(3).operation().scheduledStartTime(), is(Time.fromMilli(75)));
//    }
//
//    @Test
//    public void shouldUniformlySpreadStartTimesStartingFromBeginningOfWindowTimeFrameWhenOriginalTimesAreNotInAscendingOrder() throws OperationException {
//        HandlerFactory handlerFactory = new HandlerFactory();
//        Time windowStartTimeInclusive = Time.fromMilli(0);
//        Duration windowDuration = Duration.fromMilli(100);
//        Window.OperationHandlerTimeRangeWindow window = new Window.OperationHandlerTimeRangeWindow(windowStartTimeInclusive, windowDuration);
//
//        window.add(handlerFactory.create(Time.fromMilli(10)));
//        window.add(handlerFactory.create(Time.fromMilli(8)));
//        window.add(handlerFactory.create(Time.fromMilli(99)));
//        window.add(handlerFactory.create(Time.fromMilli(4)));
//
//        Scheduler<List<OperationHandler<?>>, Window.OperationHandlerTimeRangeWindow> scheduler = new UniformWindowedOperationHandlerScheduler();
//
//        List<OperationHandler<?>> handlers = scheduler.schedule(window);
//
//        assertThat(handlers.size(), is(4));
//        assertThat(handlers.get(0).operation().scheduledStartTime(), is(Time.fromMilli(0)));
//        assertThat(handlers.get(1).operation().scheduledStartTime(), is(Time.fromMilli(25)));
//        assertThat(handlers.get(2).operation().scheduledStartTime(), is(Time.fromMilli(50)));
//        assertThat(handlers.get(3).operation().scheduledStartTime(), is(Time.fromMilli(75)));
//    }
//
//    @Test
//    public void shouldStillUniformlySpreadStartTimesStartingFromBeginningOfWindowTimeFrameWhenTimesAreCloseTogether() throws OperationException {
//        HandlerFactory handlerFactory = new HandlerFactory();
//        Time windowStartTimeInclusive = Time.fromNano(0);
//        Duration windowDuration = Duration.fromNano(3);
//        Window.OperationHandlerTimeRangeWindow window = new Window.OperationHandlerTimeRangeWindow(windowStartTimeInclusive, windowDuration);
//
//        window.add(handlerFactory.create(Time.fromNano(0)));
//        window.add(handlerFactory.create(Time.fromNano(0)));
//        window.add(handlerFactory.create(Time.fromNano(0)));
//        window.add(handlerFactory.create(Time.fromNano(0)));
//        window.add(handlerFactory.create(Time.fromNano(0)));
//        window.add(handlerFactory.create(Time.fromNano(2)));
//
//        Scheduler<List<OperationHandler<?>>, Window.OperationHandlerTimeRangeWindow> scheduler = new UniformWindowedOperationHandlerScheduler();
//
//        List<OperationHandler<?>> handlers = scheduler.schedule(window);
//
//        assertThat(handlers.size(), is(6));
//        assertThat(handlers.get(0).operation().scheduledStartTime(), is(Time.fromNano(0)));
//        assertThat(handlers.get(1).operation().scheduledStartTime(), is(Time.fromNano(0)));
//        assertThat(handlers.get(2).operation().scheduledStartTime(), is(Time.fromNano(1)));
//        assertThat(handlers.get(3).operation().scheduledStartTime(), is(Time.fromNano(1)));
//        assertThat(handlers.get(4).operation().scheduledStartTime(), is(Time.fromNano(2)));
//        assertThat(handlers.get(5).operation().scheduledStartTime(), is(Time.fromNano(2)));
//    }

//    private static class HandlerFactory {
//        OperationHandler<?> create(Time time) throws OperationException {
//            OperationHandler<?> handler = new OperationHandler() {
//                @Override
//                protected OperationResultReport executeOperation(Operation operation) throws DbException {
//                    return null;
//                }
//            };
//            Operation operation = new NothingOperation();
//            operation.setScheduledStartTime(time);
//            handler.init(null, null, operation, null, null, null);
//            return handler;
//        }
//    }
}
