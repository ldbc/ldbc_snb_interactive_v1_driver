package com.ldbc.driver.generator;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.workloads.dummy.NothingOperation;
import com.ldbc.driver.workloads.dummy.NothingOperationHandler;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WindowGeneratorTest {
    private TimeSource timeSource = new SystemTimeSource();

    /**
     * Test With Time Range Using OperationRangeWindows
     */

    @Test
    public void shouldReturnNothingWhenOperationsIsEmptyAndReturnStrategy() {
        // Given
        Operation[] operations = new Operation[]{};
        Iterator<Operation<?>> identityGenerator = new IdentityGenerator<Operation<?>>(operations);
        final long windowDuration = 5000l;

        // When
        Iterator<Window<Operation<?>, ?>> windows = new Generator<Window<Operation<?>, ?>>() {
            private long windowStartTime = 0l;

            @Override
            protected Window<Operation<?>, ?> doNext() throws GeneratorException {
                Window<Operation<?>, ?> window = new Window.OperationTimeRangeWindow(windowStartTime, windowDuration);
                windowStartTime = windowStartTime + windowDuration;
                return window;
            }
        };
        Iterator<Window<Operation<?>, ?>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.RETURN);

        // Then
        assertThat(windowGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnAllOperationWindows() throws OperationException {
        // Given
        long[] times = new long[]{
                1000l, 3000l,
                5000l, 6000l,
                10000l,
                // nothing in this window 15-19
                20000l,
        };
        NothingOperation[] operations = new NothingOperation[times.length];
        for (int i = 0; i < operations.length; i++) {
            NothingOperation operation = new NothingOperation();
            operation.setScheduledStartTimeAsMilli(times[i]);
            operations[i] = operation;
        }

        Iterator<Operation<?>> identityGenerator = new IdentityGenerator<Operation<?>>(operations);
        final long windowDuration = 5000l;

        // When
        Iterator<Window<Operation<?>, List<Operation<?>>>> windows = new Generator<Window<Operation<?>, List<Operation<?>>>>() {
            private long windowStartTime = 0l;

            @Override
            protected Window<Operation<?>, List<Operation<?>>> doNext() throws GeneratorException {
                Window<Operation<?>, List<Operation<?>>> window = new Window.OperationTimeRangeWindow(windowStartTime, windowDuration);
                windowStartTime = windowStartTime + windowDuration;
                return window;
            }
        };
        Iterator<Window<Operation<?>, List<Operation<?>>>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.RETURN);

        // Then
        List<Operation<?>> nextOperationWindow;
        nextOperationWindow = windowGenerator.next().contents();
        assertThat(nextOperationWindow.size(), is(2));
        assertThat(nextOperationWindow.get(0).scheduledStartTimeAsMilli(), equalTo(1000l));
        assertThat(nextOperationWindow.get(1).scheduledStartTimeAsMilli(), equalTo(3000l));
        nextOperationWindow = windowGenerator.next().contents();
        assertThat(nextOperationWindow.size(), is(2));
        assertThat(nextOperationWindow.get(0).scheduledStartTimeAsMilli(), equalTo(5000l));
        assertThat(nextOperationWindow.get(1).scheduledStartTimeAsMilli(), equalTo(6000l));
        nextOperationWindow = windowGenerator.next().contents();
        assertThat(nextOperationWindow.size(), is(1));
        assertThat(nextOperationWindow.get(0).scheduledStartTimeAsMilli(), equalTo(10000l));
        nextOperationWindow = windowGenerator.next().contents();
        assertThat(nextOperationWindow.size(), is(0));
        nextOperationWindow = windowGenerator.next().contents();
        assertThat(nextOperationWindow.size(), is(1));
        assertThat(nextOperationWindow.get(0).scheduledStartTimeAsMilli(), equalTo(20000l));
        assertThat(windowGenerator.hasNext(), is(false));
    }

    /**
     * Test With Time Range Using OperationHandlerRangeWindows
     */

    @Test
    public void shouldReturnNothingWhenHandlersIsEmptyAndReturnStrategy() {
        // Given
        OperationHandler[] handlers = new OperationHandler[]{};
        Iterator<OperationHandler<?>> identityGenerator = new IdentityGenerator<OperationHandler<?>>(handlers);
        final long windowDuration = 5000l;

        // When
        Iterator<Window<OperationHandler<?>, ?>> windows = new Generator<Window<OperationHandler<?>, ?>>() {
            private long windowStartTime = 0l;

            @Override
            protected Window<OperationHandler<?>, ?> doNext() throws GeneratorException {
                Window<OperationHandler<?>, ?> window = new Window.OperationHandlerTimeRangeWindow(windowStartTime, windowDuration);
                windowStartTime = windowStartTime + windowDuration;
                return window;
            }
        };
        Iterator<Window<OperationHandler<?>, ?>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.RETURN);

        // Then
        assertThat(windowGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnAllHandlerWindows() throws OperationException {
        // Given
        long[] times = new long[]{
                1000l, 3000l,
                5000l, 6000l,
                10000l,
                // nothing in this window 15-19
                20000l,
        };
        NothingOperationHandler[] handlers = new NothingOperationHandler[times.length];
        for (int i = 0; i < handlers.length; i++) {
            NothingOperation operation = new NothingOperation();
            operation.setScheduledStartTimeAsMilli(times[i]);
            NothingOperationHandler handler = new NothingOperationHandler();
            Spinner spinner = null;
            LocalCompletionTimeWriter localCompletionTimeWriter = null;
            ConcurrentErrorReporter errorReporter = null;
            ConcurrentMetricsService metricsService = null;
            handler.init(timeSource, spinner, operation, localCompletionTimeWriter, errorReporter, metricsService);
            handlers[i] = handler;
        }

        Iterator<OperationHandler<?>> identityGenerator = new IdentityGenerator<OperationHandler<?>>(handlers);
        final long windowDuration = 5000l;

        // When
        Iterator<Window<OperationHandler<?>, List<OperationHandler<?>>>> windows = new Generator<Window<OperationHandler<?>, List<OperationHandler<?>>>>() {
            private long windowStartTime = 0l;

            @Override
            protected Window<OperationHandler<?>, List<OperationHandler<?>>> doNext() throws GeneratorException {
                Window<OperationHandler<?>, List<OperationHandler<?>>> window = new Window.OperationHandlerTimeRangeWindow(windowStartTime, windowDuration);
                windowStartTime = windowStartTime + windowDuration;
                return window;
            }
        };
        Iterator<Window<OperationHandler<?>, List<OperationHandler<?>>>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.RETURN);

        // Then
        List<OperationHandler<?>> nextHandler;
        nextHandler = windowGenerator.next().contents();
        assertThat(nextHandler.size(), is(2));
        assertThat(nextHandler.get(0).operation().scheduledStartTimeAsMilli(), equalTo(1000l));
        assertThat(nextHandler.get(1).operation().scheduledStartTimeAsMilli(), equalTo(3000l));
        nextHandler = windowGenerator.next().contents();
        assertThat(nextHandler.size(), is(2));
        assertThat(nextHandler.get(0).operation().scheduledStartTimeAsMilli(), equalTo(5000l));
        assertThat(nextHandler.get(1).operation().scheduledStartTimeAsMilli(), equalTo(6000l));
        nextHandler = windowGenerator.next().contents();
        assertThat(nextHandler.size(), is(1));
        assertThat(nextHandler.get(0).operation().scheduledStartTimeAsMilli(), equalTo(10000l));
        nextHandler = windowGenerator.next().contents();
        assertThat(nextHandler.size(), is(0));
        nextHandler = windowGenerator.next().contents();
        assertThat(nextHandler.size(), is(1));
        assertThat(nextHandler.get(0).operation().scheduledStartTimeAsMilli(), equalTo(20000l));
        assertThat(windowGenerator.hasNext(), is(false));
    }

    /**
     * Test With Time Range Using PredicateWindows
     */

    @Test
    public void shouldReturnNothingWhenTimesIsEmptyAndReturnStrategy() {
        // Given
        Long[] times = new Long[]{};
        Iterator<Long> identityGenerator = new IdentityGenerator<>(times);
        final long windowDuration = 5000l;

        // When
        Iterator<Window<Long, ?>> windows = new Generator<Window<Long, ?>>() {
            private Long windowStartTime = 0l;

            @Override
            protected Window<Long, ?> doNext() throws GeneratorException {
                Predicate<Long> timeInRange = new Predicate<Long>() {
                    @Override
                    public boolean apply(Long time) {
                        return time >= windowStartTime && time < (windowStartTime + windowDuration);
                    }
                };
                windowStartTime = windowStartTime + windowDuration;
                return new Window.PredicateWindow<>(timeInRange);
            }
        };
        Iterator<Window<Long, ?>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.RETURN);

        // Then
        assertThat(windowGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnAllTimeWindows() {
        // Given
        Long[] times = new Long[]{
                1000l, 3000l,
                5000l, 6000l,
                10000l};
        Iterator<Long> identityGenerator = new IdentityGenerator<>(times);
        final long windowDuration = 5000l;

        // When
        Iterator<Window<Long, List<Long>>> windows = new Generator<Window<Long, List<Long>>>() {
            private long windowStartTime = 0l;

            @Override
            protected Window<Long, List<Long>> doNext() throws GeneratorException {
                Predicate<Long> timeInRange = new Predicate<Long>() {
                    private Long myWindowStartTime = windowStartTime;

                    @Override
                    public boolean apply(Long time) {
                        return time >= myWindowStartTime && time < (myWindowStartTime + windowDuration);
                    }
                };
                windowStartTime = windowStartTime + windowDuration;
                return new Window.PredicateWindow<>(timeInRange);
            }
        };
        Iterator<Window<Long, List<Long>>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.RETURN);

        // Then
        assertThat(windowGenerator.next().contents(), equalTo((List<Long>) Lists.newArrayList(1000l, 3000l)));
        assertThat(windowGenerator.next().contents(), equalTo((List<Long>) Lists.newArrayList(5000l, 6000l)));
        assertThat(windowGenerator.next().contents(), equalTo((List<Long>) Lists.newArrayList(10000l)));
        assertThat(windowGenerator.hasNext(), is(false));

    }

    /**
     * Test With Sized Windows
     */

    @Test
    public void shouldReturnNothingWhenThingsIsEmptyAndReturnStrategy() {
        // Given
        Integer[] numbers = new Integer[]{};
        Iterator<Integer> identityGenerator = new IdentityGenerator<>(numbers);

        // When
        Iterator<Window<Integer, ?>> windows = new Generator<Window<Integer, ?>>() {
            @Override
            protected Window<Integer, ?> doNext() throws GeneratorException {
                return new Window.SizeWindow<>(2);
            }
        };
        Iterator<Window<Integer, ?>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.RETURN);

        // Then
        assertThat(windowGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnNothingWhenThingsIsEmptyAndDiscardStrategy() {
        // Given
        Integer[] numbers = new Integer[]{};
        Iterator<Integer> identityGenerator = new IdentityGenerator<>(numbers);

        // When
        Iterator<Window<Integer, ?>> windows = new Generator<Window<Integer, ?>>() {
            @Override
            protected Window<Integer, ?> doNext() throws GeneratorException {
                return new Window.SizeWindow<>(2);
            }
        };
        Iterator<Window<Integer, ?>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.DISCARD);

        // Then
        assertThat(windowGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnNothingWhenThingsIsEmptyAndErrorStrategy() {
        // Given
        Integer[] numbers = new Integer[]{};
        Iterator<Integer> identityGenerator = new IdentityGenerator<>(numbers);

        // When
        Iterator<Window<Integer, ?>> windows = new Generator<Window<Integer, ?>>() {
            @Override
            protected Window<Integer, ?> doNext() throws GeneratorException {
                return new Window.SizeWindow<>(2);
            }
        };
        Iterator<Window<Integer, ?>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.ERROR);

        // Then
        assertThat(windowGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnAllWindowsIncludingPartialWhenPartialExists() {
        // Given
        Integer[] numbers = new Integer[]{1, 2, 3, 4, 5, 6, 7};
        Iterator<Integer> identityGenerator = new IdentityGenerator<>(numbers);

        // When
        Iterator<Window<Integer, List<Integer>>> windows = new Generator<Window<Integer, List<Integer>>>() {
            @Override
            protected Window<Integer, List<Integer>> doNext() throws GeneratorException {
                return new Window.SizeWindow<>(2);
            }
        };
        Iterator<Window<Integer, List<Integer>>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.RETURN);

        // Then
        assertThat(windowGenerator.next().contents(), equalTo((List<Integer>) Lists.newArrayList(1, 2)));
        assertThat(windowGenerator.next().contents(), equalTo((List<Integer>) Lists.newArrayList(3, 4)));
        assertThat(windowGenerator.next().contents(), equalTo((List<Integer>) Lists.newArrayList(5, 6)));
        assertThat(windowGenerator.next().contents(), equalTo((List<Integer>) Lists.newArrayList(7)));
        assertThat(windowGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnAllWindowsIncludingPartialWhenNoPartialExists() {
        // Given
        Integer[] numbers = new Integer[]{1, 2, 3, 4, 5, 6};
        Iterator<Integer> identityGenerator = new IdentityGenerator<>(numbers);

        // When
        Iterator<Window<Integer, List<Integer>>> windows = new Generator<Window<Integer, List<Integer>>>() {
            @Override
            protected Window<Integer, List<Integer>> doNext() throws GeneratorException {
                return new Window.SizeWindow<>(2);
            }
        };
        Iterator<Window<Integer, List<Integer>>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.RETURN);

        // Then
        assertThat(windowGenerator.next().contents(), equalTo((List<Integer>) Lists.newArrayList(1, 2)));
        assertThat(windowGenerator.next().contents(), equalTo((List<Integer>) Lists.newArrayList(3, 4)));
        assertThat(windowGenerator.next().contents(), equalTo((List<Integer>) Lists.newArrayList(5, 6)));
        assertThat(windowGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnAllWindowsExcludingPartialWhenNoPartialExists() {
        // Given
        Integer[] numbers = new Integer[]{1, 2, 3, 4, 5, 6};
        Iterator<Integer> identityGenerator = new IdentityGenerator<>(numbers);

        // When
        Iterator<Window<Integer, List<Integer>>> windows = new Generator<Window<Integer, List<Integer>>>() {
            @Override
            protected Window<Integer, List<Integer>> doNext() throws GeneratorException {
                return new Window.SizeWindow<>(2);
            }
        };
        Iterator<Window<Integer, List<Integer>>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.DISCARD);

        // Then
        assertThat(windowGenerator.next().contents(), equalTo((List<Integer>) Lists.newArrayList(1, 2)));
        assertThat(windowGenerator.next().contents(), equalTo((List<Integer>) Lists.newArrayList(3, 4)));
        assertThat(windowGenerator.next().contents(), equalTo((List<Integer>) Lists.newArrayList(5, 6)));
        assertThat(windowGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnAllWindowsExcludingPartialWhenPartialExists() {
        // Given
        Integer[] numbers = new Integer[]{1, 2, 3, 4, 5, 6, 7};
        Iterator<Integer> identityGenerator = new IdentityGenerator<>(numbers);

        // When
        Iterator<Window<Integer, List<Integer>>> windows = new Generator<Window<Integer, List<Integer>>>() {
            @Override
            protected Window<Integer, List<Integer>> doNext() throws GeneratorException {
                return new Window.SizeWindow<>(2);
            }
        };
        Iterator<Window<Integer, List<Integer>>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.DISCARD);

        // Then
        assertThat(windowGenerator.next().contents(), equalTo((List<Integer>) Lists.newArrayList(1, 2)));
        assertThat(windowGenerator.next().contents(), equalTo((List<Integer>) Lists.newArrayList(3, 4)));
        assertThat(windowGenerator.next().contents(), equalTo((List<Integer>) Lists.newArrayList(5, 6)));
        assertThat(windowGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnAllWindowsThenThrowExceptionIfPartialExistsWhenNoPartialPresent() {
        // Given
        Integer[] numbers = new Integer[]{1, 2, 3, 4, 5, 6};
        Iterator<Integer> identityGenerator = new IdentityGenerator<>(numbers);

        // When
        Iterator<Window<Integer, List<Integer>>> windows = new Generator<Window<Integer, List<Integer>>>() {
            @Override
            protected Window<Integer, List<Integer>> doNext() throws GeneratorException {
                return new Window.SizeWindow<>(2);
            }
        };
        Iterator<Window<Integer, List<Integer>>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.ERROR);

        // Then
        assertThat(windowGenerator.next().contents(), equalTo((List<Integer>) Lists.newArrayList(1, 2)));
        assertThat(windowGenerator.next().contents(), equalTo((List<Integer>) Lists.newArrayList(3, 4)));
        assertThat(windowGenerator.next().contents(), equalTo((List<Integer>) Lists.newArrayList(5, 6)));
        boolean exceptionWasThrown = false;
        try {
            assertThat(windowGenerator.hasNext(), is(false));
        } catch (GeneratorException e) {
            exceptionWasThrown = true;
        }
        assertThat(exceptionWasThrown, is(false));
    }

    @Test
    public void shouldReturnAllWindowsThenThrowExceptionIfPartialExistsWhenPartialPresent() {
        // Given
        Integer[] numbers = new Integer[]{1, 2, 3, 4, 5, 6, 7};
        Iterator<Integer> identityGenerator = new IdentityGenerator<>(numbers);

        // When
        Iterator<Window<Integer, List<Integer>>> windows = new Generator<Window<Integer, List<Integer>>>() {
            @Override
            protected Window<Integer, List<Integer>> doNext() throws GeneratorException {
                return new Window.SizeWindow<>(2);
            }
        };
        Iterator<Window<Integer, List<Integer>>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.ERROR);

        // Then
        assertThat(windowGenerator.next().contents(), equalTo((List<Integer>) Lists.newArrayList(1, 2)));
        assertThat(windowGenerator.next().contents(), equalTo((List<Integer>) Lists.newArrayList(3, 4)));
        assertThat(windowGenerator.next().contents(), equalTo((List<Integer>) Lists.newArrayList(5, 6)));
        boolean exceptionWasThrown = false;
        try {
            assertThat(windowGenerator.hasNext(), is(false));
        } catch (GeneratorException e) {
            exceptionWasThrown = true;
        }
        assertThat(exceptionWasThrown, is(true));
    }

    /**
     * Test With Single Item Windows
     */

    @Test
    public void shouldReturnNothingWhenThingsIsEmptyAndReturnStrategyWithSingleItemWindow() {
        // Given
        Integer[] numbers = new Integer[]{};
        Iterator<Integer> identityGenerator = new IdentityGenerator<>(numbers);

        // When
        Iterator<Window<Integer, Integer>> windows = new Generator<Window<Integer, Integer>>() {
            @Override
            protected Window<Integer, Integer> doNext() throws GeneratorException {
                return new Window.SingleItemWindow<>();
            }
        };
        Iterator<Window<Integer, Integer>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.RETURN);

        // Then
        assertThat(windowGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnNothingWhenThingsIsEmptyAndDiscardStrategyWithSingleItemWindow() {
        // Given
        Integer[] numbers = new Integer[]{};
        Iterator<Integer> identityGenerator = new IdentityGenerator<>(numbers);

        // When
        Iterator<Window<Integer, Integer>> windows = new Generator<Window<Integer, Integer>>() {
            @Override
            protected Window<Integer, Integer> doNext() throws GeneratorException {
                return new Window.SingleItemWindow<>();
            }
        };
        Iterator<Window<Integer, Integer>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.DISCARD);

        // Then
        assertThat(windowGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnNothingWhenThingsIsEmptyAndErrorStrategyWithSingleItemWindow() {
        // Given
        Integer[] numbers = new Integer[]{};
        Iterator<Integer> identityGenerator = new IdentityGenerator<>(numbers);

        // When
        Iterator<Window<Integer, Integer>> windows = new Generator<Window<Integer, Integer>>() {
            @Override
            protected Window<Integer, Integer> doNext() throws GeneratorException {
                return new Window.SingleItemWindow<>();
            }
        };
        Iterator<Window<Integer, Integer>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.ERROR);

        // Then
        assertThat(windowGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnAllWindowsIncludingPartialWhenPartialExistsWithSingleItemWindow() {
        // Given
        Integer[] numbers = new Integer[]{1, 2, 3, 4, 5, 6, 7};
        Iterator<Integer> identityGenerator = new IdentityGenerator<>(numbers);

        // When
        Iterator<Window<Integer, Integer>> windows = new Generator<Window<Integer, Integer>>() {
            @Override
            protected Window<Integer, Integer> doNext() throws GeneratorException {
                return new Window.SingleItemWindow<>();
            }
        };
        Iterator<Window<Integer, Integer>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.RETURN);


        // Then
        assertThat(windowGenerator.next().contents(), is(1));
        assertThat(windowGenerator.next().contents(), is(2));
        assertThat(windowGenerator.next().contents(), is(3));
        assertThat(windowGenerator.next().contents(), is(4));
        assertThat(windowGenerator.next().contents(), is(5));
        assertThat(windowGenerator.next().contents(), is(6));
        assertThat(windowGenerator.next().contents(), is(7));
        assertThat(windowGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnAllWindowsIncludingPartialWhenNoPartialExistsWithSingleItemWindow() {
        // Given
        Integer[] numbers = new Integer[]{1, 2, 3, 4, 5, 6};
        Iterator<Integer> identityGenerator = new IdentityGenerator<>(numbers);

        // When
        Iterator<Window<Integer, Integer>> windows = new Generator<Window<Integer, Integer>>() {
            @Override
            protected Window<Integer, Integer> doNext() throws GeneratorException {
                return new Window.SingleItemWindow<>();
            }
        };
        Iterator<Window<Integer, Integer>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.RETURN);

        // Then
        assertThat(windowGenerator.next().contents(), is(1));
        assertThat(windowGenerator.next().contents(), is(2));
        assertThat(windowGenerator.next().contents(), is(3));
        assertThat(windowGenerator.next().contents(), is(4));
        assertThat(windowGenerator.next().contents(), is(5));
        assertThat(windowGenerator.next().contents(), is(6));
        assertThat(windowGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnAllWindowsExcludingPartialWhenNoPartialExistsWithSingleItemWindow() {
        // Given
        Integer[] numbers = new Integer[]{1, 2, 3, 4, 5, 6};
        Iterator<Integer> identityGenerator = new IdentityGenerator<>(numbers);

        // When
        final Window.SingleItemWindow window = new Window.SingleItemWindow<Integer>();
        Iterator<Window<Integer, Integer>> windows = new Generator<Window<Integer, Integer>>() {
            @Override
            protected Window<Integer, Integer> doNext() throws GeneratorException {
                window.reset();
                return window;
            }
        };
        Iterator<Window<Integer, Integer>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.DISCARD);

        // Then
        assertThat(windowGenerator.next().contents(), is(1));
        assertThat(windowGenerator.next().contents(), is(2));
        assertThat(windowGenerator.next().contents(), is(3));
        assertThat(windowGenerator.next().contents(), is(4));
        assertThat(windowGenerator.next().contents(), is(5));
        assertThat(windowGenerator.next().contents(), is(6));
        assertThat(windowGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnAllWindowsExcludingPartialWhenPartialExistsWithSingleItemWindow() {
        // Given
        Integer[] numbers = new Integer[]{1, 2, 3, 4, 5, 6, 7};
        Iterator<Integer> identityGenerator = new IdentityGenerator<>(numbers);

        // When
        Iterator<Window<Integer, Integer>> windows = new Generator<Window<Integer, Integer>>() {
            @Override
            protected Window<Integer, Integer> doNext() throws GeneratorException {
                return new Window.SingleItemWindow<>();
            }
        };
        Iterator<Window<Integer, Integer>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.DISCARD);

        // Then
        assertThat(windowGenerator.next().contents(), is(1));
        assertThat(windowGenerator.next().contents(), is(2));
        assertThat(windowGenerator.next().contents(), is(3));
        assertThat(windowGenerator.next().contents(), is(4));
        assertThat(windowGenerator.next().contents(), is(5));
        assertThat(windowGenerator.next().contents(), is(6));
        assertThat(windowGenerator.next().contents(), is(7));
        assertThat(windowGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnAllWindowsThenThrowExceptionIfPartialExistsWhenNoPartialPresentWithSingleItemWindow() {
        // Given
        Integer[] numbers = new Integer[]{1, 2, 3, 4, 5, 6};
        Iterator<Integer> identityGenerator = new IdentityGenerator<>(numbers);

        // When
        Iterator<Window<Integer, Integer>> windows = new Generator<Window<Integer, Integer>>() {
            @Override
            protected Window<Integer, Integer> doNext() throws GeneratorException {
                return new Window.SingleItemWindow<>();
            }
        };
        Iterator<Window<Integer, Integer>> windowGenerator = new WindowGenerator<>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.ERROR);

        // Then
        assertThat(windowGenerator.next().contents(), is(1));
        assertThat(windowGenerator.next().contents(), is(2));
        assertThat(windowGenerator.next().contents(), is(3));
        assertThat(windowGenerator.next().contents(), is(4));
        assertThat(windowGenerator.next().contents(), is(5));
        assertThat(windowGenerator.next().contents(), is(6));
        boolean exceptionWasThrown = false;
        try {
            assertThat(windowGenerator.hasNext(), is(false));
        } catch (GeneratorException e) {
            exceptionWasThrown = true;
        }
        assertThat(exceptionWasThrown, is(false));
    }
}