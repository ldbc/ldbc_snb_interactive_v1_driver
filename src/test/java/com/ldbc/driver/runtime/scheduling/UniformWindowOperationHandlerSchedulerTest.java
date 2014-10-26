package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.*;
import com.ldbc.driver.generator.Window;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.workloads.dummy.DummyDb;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class UniformWindowOperationHandlerSchedulerTest {
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();

    @Test
    public void shouldNotCrashWithEmptyWindow() throws OperationException {
        long windowStartTimeInclusive = 0l;
        long windowDuration = 100l;
        Window.OperationHandlerTimeRangeWindow window = new Window.OperationHandlerTimeRangeWindow(windowStartTimeInclusive, windowDuration);

        Scheduler<List<OperationHandler<?>>, Window.OperationHandlerTimeRangeWindow> scheduler = new UniformWindowedOperationHandlerScheduler();

        List<OperationHandler<?>> handlers = scheduler.schedule(window);

        assertThat(handlers.size(), is(0));
    }

    @Test
    public void shouldUniformlySpreadStartTimesStartingFromBeginningOfWindowTimeFrame() throws OperationException, DbException, IOException {
        long windowStartTimeInclusive = 0l;
        long windowDuration = 100l;
        Window.OperationHandlerTimeRangeWindow window = new Window.OperationHandlerTimeRangeWindow(windowStartTimeInclusive, windowDuration);

        Db db = new DummyDb();
        db.init(new HashMap<String, String>());
        OperationHandler<?> operationHandler1 = null;
        OperationHandler<?> operationHandler2 = null;
        OperationHandler<?> operationHandler3 = null;
        OperationHandler<?> operationHandler4 = null;
        try {
            Operation<?> operation1 = new TimedNamedOperation1(10l, 0l, "name");
            Operation<?> operation2 = new TimedNamedOperation1(11l, 0l, "name");
            Operation<?> operation3 = new TimedNamedOperation1(12l, 0l, "name");
            Operation<?> operation4 = new TimedNamedOperation1(13l, 0l, "name");

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
            assertThat(handlers.get(0).operation().scheduledStartTimeAsMilli(), is(0l));
            assertThat(handlers.get(1).operation().scheduledStartTimeAsMilli(), is(25l));
            assertThat(handlers.get(2).operation().scheduledStartTimeAsMilli(), is(50l));
            assertThat(handlers.get(3).operation().scheduledStartTimeAsMilli(), is(75l));
        } finally {
            if (null != operationHandler1) operationHandler1.cleanup();
            if (null != operationHandler2) operationHandler2.cleanup();
            if (null != operationHandler3) operationHandler3.cleanup();
            if (null != operationHandler4) operationHandler4.cleanup();
            if (null != db) db.close();
        }
    }

    @Test
    public void shouldUniformlySpreadStartTimesStartingFromBeginningOfWindowTimeFrameWhenOriginalTimesAreNotInAscendingOrder() throws OperationException, DbException, IOException {
        long windowStartTimeInclusive = 0l;
        long windowDuration = 100l;
        Window.OperationHandlerTimeRangeWindow window = new Window.OperationHandlerTimeRangeWindow(windowStartTimeInclusive, windowDuration);

        Db db = new DummyDb();
        db.init(new HashMap<String, String>());
        OperationHandler<?> operationHandler1 = null;
        OperationHandler<?> operationHandler2 = null;
        OperationHandler<?> operationHandler3 = null;
        OperationHandler<?> operationHandler4 = null;
        try {
            Operation<?> operation1 = new TimedNamedOperation1(10l, 0l, "name");
            Operation<?> operation2 = new TimedNamedOperation1(8l, 0l, "name");
            Operation<?> operation3 = new TimedNamedOperation1(99l, 0l, "name");
            Operation<?> operation4 = new TimedNamedOperation1(4l, 0l, "name");

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
            assertThat(handlers.get(0).operation().scheduledStartTimeAsMilli(), is(0l));
            assertThat(handlers.get(1).operation().scheduledStartTimeAsMilli(), is(25l));
            assertThat(handlers.get(2).operation().scheduledStartTimeAsMilli(), is(50l));
            assertThat(handlers.get(3).operation().scheduledStartTimeAsMilli(), is(75l));
        } finally {
            if (null != operationHandler1) operationHandler1.cleanup();
            if (null != operationHandler2) operationHandler2.cleanup();
            if (null != operationHandler3) operationHandler3.cleanup();
            if (null != operationHandler4) operationHandler4.cleanup();
            if (null != db) db.close();
        }
    }

    @Test
    public void shouldStillUniformlySpreadStartTimesStartingFromBeginningOfWindowTimeFrameWhenTimesAreCloseTogether() throws OperationException, DbException, IOException {
        long windowStartTimeInclusive = 0l;
        long windowDuration = 3l;
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
            Operation<?> operation1 = new TimedNamedOperation1(0l, 0l, "name");
            Operation<?> operation2 = new TimedNamedOperation1(0l, 0l, "name");
            Operation<?> operation3 = new TimedNamedOperation1(0l, 0l, "name");
            Operation<?> operation4 = new TimedNamedOperation1(0l, 0l, "name");
            Operation<?> operation5 = new TimedNamedOperation1(0l, 0l, "name");
            Operation<?> operation6 = new TimedNamedOperation1(2l, 0l, "name");

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
            assertThat(handlers.get(0).operation().scheduledStartTimeAsMilli(), is(0l));
            assertThat(handlers.get(1).operation().scheduledStartTimeAsMilli(), is(0l));
            assertThat(handlers.get(2).operation().scheduledStartTimeAsMilli(), is(1l));
            assertThat(handlers.get(3).operation().scheduledStartTimeAsMilli(), is(1l));
            assertThat(handlers.get(4).operation().scheduledStartTimeAsMilli(), is(2l));
            assertThat(handlers.get(5).operation().scheduledStartTimeAsMilli(), is(2l));
        } finally {
            if (null != operationHandler1) operationHandler1.cleanup();
            if (null != operationHandler2) operationHandler2.cleanup();
            if (null != operationHandler3) operationHandler3.cleanup();
            if (null != operationHandler4) operationHandler4.cleanup();
            if (null != operationHandler5) operationHandler5.cleanup();
            if (null != operationHandler6) operationHandler6.cleanup();
            if (null != db) db.close();
        }
    }
}
