package com.ldbc.driver.generator;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.error.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.executor.AlwaysValidCompletionTimeValidator;
import com.ldbc.driver.runtime.executor.CompletionTimeValidator;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WindowGeneratorTest {

    /**
     * Test With Time Range Using OperationHandlerRangeWindows
     */

    @Test
    public void shouldReturnNothingWhenHandlersIsEmptyAndReturnStrategy() {
        // Given
        OperationHandler[] handlers = new OperationHandler[]{};
        Iterator<OperationHandler<?>> identityGenerator = new IdentityGenerator<OperationHandler<?>>(handlers);
        final Duration windowDuration = Duration.fromSeconds(5);

        // When
        Iterator<Window<OperationHandler<?>, ?>> windows = new Generator<Window<OperationHandler<?>, ?>>() {
            private Time windowStartTime = Time.fromSeconds(0);

            @Override
            protected Window<OperationHandler<?>, ?> doNext() throws GeneratorException {
                Window<OperationHandler<?>, ?> window = new Window.OperationHandlerTimeRangeWindow(windowStartTime, windowDuration);
                windowStartTime = windowStartTime.plus(windowDuration);
                return window;
            }
        };
        Iterator<Window<OperationHandler<?>, ?>> windowGenerator = new WindowGenerator<OperationHandler<?>, Window<OperationHandler<?>, ?>>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.RETURN);

        // Then
        assertThat(windowGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnAllHandlerWindows() {
        // Given
        Time[] times = new Time[]{
                Time.fromSeconds(1), Time.fromSeconds(3),
                Time.fromSeconds(5), Time.fromSeconds(6),
                Time.fromSeconds(10),
                // nothing in this window 15-19
                Time.fromSeconds(20),
        };
        TestOperationHandler[] handlers = new TestOperationHandler[times.length];
        for (int i = 0; i < handlers.length; i++) {
            TestOperation operation = new TestOperation();
            operation.setScheduledStartTime(times[i]);
            TestOperationHandler handler = new TestOperationHandler();
            AlwaysValidCompletionTimeValidator.Spinner spinner = null;
            ConcurrentCompletionTimeService completionTimeService = null;
            ConcurrentErrorReporter errorReporter = null;
            ConcurrentMetricsService metricsService = null;
            CompletionTimeValidator completionTimeValidator = null;
            handler.init(spinner, operation, completionTimeService, errorReporter, metricsService, completionTimeValidator);
            handlers[i] = handler;
        }

        Iterator<OperationHandler<?>> identityGenerator = new IdentityGenerator<OperationHandler<?>>(handlers);
        final Duration windowDuration = Duration.fromSeconds(5);

        // When
        Iterator<Window<OperationHandler<?>, List<OperationHandler<?>>>> windows = new Generator<Window<OperationHandler<?>, List<OperationHandler<?>>>>() {
            private Time windowStartTime = Time.fromSeconds(0);

            @Override
            protected Window<OperationHandler<?>, List<OperationHandler<?>>> doNext() throws GeneratorException {
                Window<OperationHandler<?>, List<OperationHandler<?>>> window = new Window.OperationHandlerTimeRangeWindow(windowStartTime, windowDuration);
                windowStartTime = windowStartTime.plus(windowDuration);
                return window;
            }
        };
        Iterator<Window<OperationHandler<?>, List<OperationHandler<?>>>> windowGenerator =
                new WindowGenerator<OperationHandler<?>, Window<OperationHandler<?>, List<OperationHandler<?>>>>(
                        identityGenerator,
                        windows,
                        WindowGenerator.PartialWindowStrategy.RETURN);

        // Then
        List<OperationHandler<?>> nextHandler;
        nextHandler = windowGenerator.next().contents();
        assertThat(nextHandler.size(), is(2));
        assertThat(nextHandler.get(0).operation().scheduledStartTime(), equalTo(Time.fromSeconds(1)));
        assertThat(nextHandler.get(1).operation().scheduledStartTime(), equalTo(Time.fromSeconds(3)));
        nextHandler = windowGenerator.next().contents();
        assertThat(nextHandler.size(), is(2));
        assertThat(nextHandler.get(0).operation().scheduledStartTime(), equalTo(Time.fromSeconds(5)));
        assertThat(nextHandler.get(1).operation().scheduledStartTime(), equalTo(Time.fromSeconds(6)));
        nextHandler = windowGenerator.next().contents();
        assertThat(nextHandler.size(), is(1));
        assertThat(nextHandler.get(0).operation().scheduledStartTime(), equalTo(Time.fromSeconds(10)));
        nextHandler = windowGenerator.next().contents();
        assertThat(nextHandler.size(), is(0));
        nextHandler = windowGenerator.next().contents();
        assertThat(nextHandler.size(), is(1));
        assertThat(nextHandler.get(0).operation().scheduledStartTime(), equalTo(Time.fromSeconds(20)));
        assertThat(windowGenerator.hasNext(), is(false));
    }

    private static class TestOperationHandler extends OperationHandler<TestOperation> {
        @Override
        protected OperationResult executeOperation(TestOperation operation) throws DbException {
            return null;
        }
    }

    private static class TestOperation extends Operation<Object> {
    }

    /**
     * Test With Time Range Using PredicateWindows
     */

    @Test
    public void shouldReturnNothingWhenTimesIsEmptyAndReturnStrategy() {
        // Given
        Time[] times = new Time[]{};
        Iterator<Time> identityGenerator = new IdentityGenerator<Time>(times);
        final Duration windowDuration = Duration.fromSeconds(5);

        // When
        Iterator<Window<Time, ?>> windows = new Generator<Window<Time, ?>>() {
            private Time windowStartTime = Time.fromSeconds(0);

            @Override
            protected Window<Time, ?> doNext() throws GeneratorException {
                Predicate<Time> timeInRange = new Predicate<Time>() {
                    @Override
                    public boolean apply(Time time) {
                        return time.gte(windowStartTime) && time.lt(windowStartTime.plus(windowDuration));
                    }
                };
                windowStartTime = windowStartTime.plus(windowDuration);
                return new Window.PredicateWindow<Time>(timeInRange);
            }
        };
        Iterator<Window<Time, ?>> windowGenerator = new WindowGenerator<Time, Window<Time, ?>>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.RETURN);

        // Then
        assertThat(windowGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnAllTimeWindows() {
        // Given
        Time[] times = new Time[]{
                Time.fromSeconds(1), Time.fromSeconds(3),
                Time.fromSeconds(5), Time.fromSeconds(6),
                Time.fromSeconds(10)};
        Iterator<Time> identityGenerator = new IdentityGenerator<Time>(times);
        final Duration windowDuration = Duration.fromSeconds(5);

        // When
        Iterator<Window<Time, List<Time>>> windows = new Generator<Window<Time, List<Time>>>() {
            private Time windowStartTime = Time.fromSeconds(0);

            @Override
            protected Window<Time, List<Time>> doNext() throws GeneratorException {
                Predicate<Time> timeInRange = new Predicate<Time>() {
                    private Time myWindowStartTime = windowStartTime;

                    @Override
                    public boolean apply(Time time) {
                        return time.gte(myWindowStartTime) && time.lt(myWindowStartTime.plus(windowDuration));
                    }
                };
                windowStartTime = windowStartTime.plus(windowDuration);
                return new Window.PredicateWindow<Time>(timeInRange);
            }
        };
        Iterator<Window<Time, List<Time>>> windowGenerator = new WindowGenerator<Time, Window<Time, List<Time>>>(
                identityGenerator,
                windows,
                WindowGenerator.PartialWindowStrategy.RETURN);

        // Then
        assertThat(windowGenerator.next().contents(), equalTo((List<Time>) Lists.newArrayList(Time.fromSeconds(1), Time.fromSeconds(3))));
        assertThat(windowGenerator.next().contents(), equalTo((List<Time>) Lists.newArrayList(Time.fromSeconds(5), Time.fromSeconds(6))));
        assertThat(windowGenerator.next().contents(), equalTo((List<Time>) Lists.newArrayList(Time.fromSeconds(10))));
        assertThat(windowGenerator.hasNext(), is(false));

    }

    /**
     * Test With Sized Windows
     */

    @Test
    public void shouldReturnNothingWhenThingsIsEmptyAndReturnStrategy() {
        // Given
        Integer[] numbers = new Integer[]{};
        Iterator<Integer> identityGenerator = new IdentityGenerator<Integer>(numbers);

        // When
        Iterator<Window<Integer, ?>> windows = new Generator<Window<Integer, ?>>() {
            @Override
            protected Window<Integer, ?> doNext() throws GeneratorException {
                return new Window.SizeWindow<Integer>(2);
            }
        };
        Iterator<Window<Integer, ?>> windowGenerator = new WindowGenerator<Integer, Window<Integer, ?>>(
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
        Iterator<Integer> identityGenerator = new IdentityGenerator<Integer>(numbers);

        // When
        Iterator<Window<Integer, ?>> windows = new Generator<Window<Integer, ?>>() {
            @Override
            protected Window<Integer, ?> doNext() throws GeneratorException {
                return new Window.SizeWindow<Integer>(2);
            }
        };
        Iterator<Window<Integer, ?>> windowGenerator = new WindowGenerator<Integer, Window<Integer, ?>>(
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
        Iterator<Integer> identityGenerator = new IdentityGenerator<Integer>(numbers);

        // When
        Iterator<Window<Integer, ?>> windows = new Generator<Window<Integer, ?>>() {
            @Override
            protected Window<Integer, ?> doNext() throws GeneratorException {
                return new Window.SizeWindow<Integer>(2);
            }
        };
        Iterator<Window<Integer, ?>> windowGenerator = new WindowGenerator<Integer, Window<Integer, ?>>(
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
        Iterator<Integer> identityGenerator = new IdentityGenerator<Integer>(numbers);

        // When
        Iterator<Window<Integer, List<Integer>>> windows = new Generator<Window<Integer, List<Integer>>>() {
            @Override
            protected Window<Integer, List<Integer>> doNext() throws GeneratorException {
                return new Window.SizeWindow<Integer>(2);
            }
        };
        Iterator<Window<Integer, List<Integer>>> windowGenerator = new WindowGenerator<Integer, Window<Integer, List<Integer>>>(
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
        Iterator<Integer> identityGenerator = new IdentityGenerator<Integer>(numbers);

        // When
        Iterator<Window<Integer, List<Integer>>> windows = new Generator<Window<Integer, List<Integer>>>() {
            @Override
            protected Window<Integer, List<Integer>> doNext() throws GeneratorException {
                return new Window.SizeWindow<Integer>(2);
            }
        };
        Iterator<Window<Integer, List<Integer>>> windowGenerator = new WindowGenerator<Integer, Window<Integer, List<Integer>>>(
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
        Iterator<Integer> identityGenerator = new IdentityGenerator<Integer>(numbers);

        // When
        Iterator<Window<Integer, List<Integer>>> windows = new Generator<Window<Integer, List<Integer>>>() {
            @Override
            protected Window<Integer, List<Integer>> doNext() throws GeneratorException {
                return new Window.SizeWindow<Integer>(2);
            }
        };
        Iterator<Window<Integer, List<Integer>>> windowGenerator = new WindowGenerator<Integer, Window<Integer, List<Integer>>>(
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
        Iterator<Integer> identityGenerator = new IdentityGenerator<Integer>(numbers);

        // When
        Iterator<Window<Integer, List<Integer>>> windows = new Generator<Window<Integer, List<Integer>>>() {
            @Override
            protected Window<Integer, List<Integer>> doNext() throws GeneratorException {
                return new Window.SizeWindow<Integer>(2);
            }
        };
        Iterator<Window<Integer, List<Integer>>> windowGenerator = new WindowGenerator<Integer, Window<Integer, List<Integer>>>(
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
        Iterator<Integer> identityGenerator = new IdentityGenerator<Integer>(numbers);

        // When
        Iterator<Window<Integer, List<Integer>>> windows = new Generator<Window<Integer, List<Integer>>>() {
            @Override
            protected Window<Integer, List<Integer>> doNext() throws GeneratorException {
                return new Window.SizeWindow<Integer>(2);
            }
        };
        Iterator<Window<Integer, List<Integer>>> windowGenerator = new WindowGenerator<Integer, Window<Integer, List<Integer>>>(
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
        Iterator<Integer> identityGenerator = new IdentityGenerator<Integer>(numbers);

        // When
        Iterator<Window<Integer, List<Integer>>> windows = new Generator<Window<Integer, List<Integer>>>() {
            @Override
            protected Window<Integer, List<Integer>> doNext() throws GeneratorException {
                return new Window.SizeWindow<Integer>(2);
            }
        };
        Iterator<Window<Integer, List<Integer>>> windowGenerator = new WindowGenerator<Integer, Window<Integer, List<Integer>>>(
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
        Iterator<Integer> identityGenerator = new IdentityGenerator<Integer>(numbers);

        // When
        Iterator<Window<Integer, Integer>> windows = new Generator<Window<Integer, Integer>>() {
            @Override
            protected Window<Integer, Integer> doNext() throws GeneratorException {
                return new Window.SingleItemWindow<Integer>();
            }
        };
        Iterator<Window<Integer, Integer>> windowGenerator = new WindowGenerator<Integer, Window<Integer, Integer>>(
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
        Iterator<Integer> identityGenerator = new IdentityGenerator<Integer>(numbers);

        // When
        Iterator<Window<Integer, Integer>> windows = new Generator<Window<Integer, Integer>>() {
            @Override
            protected Window<Integer, Integer> doNext() throws GeneratorException {
                return new Window.SingleItemWindow<Integer>();
            }
        };
        Iterator<Window<Integer, Integer>> windowGenerator = new WindowGenerator<Integer, Window<Integer, Integer>>(
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
        Iterator<Integer> identityGenerator = new IdentityGenerator<Integer>(numbers);

        // When
        Iterator<Window<Integer, Integer>> windows = new Generator<Window<Integer, Integer>>() {
            @Override
            protected Window<Integer, Integer> doNext() throws GeneratorException {
                return new Window.SingleItemWindow<Integer>();
            }
        };
        Iterator<Window<Integer, Integer>> windowGenerator = new WindowGenerator<Integer, Window<Integer, Integer>>(
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
        Iterator<Integer> identityGenerator = new IdentityGenerator<Integer>(numbers);

        // When
        Iterator<Window<Integer, Integer>> windows = new Generator<Window<Integer, Integer>>() {
            @Override
            protected Window<Integer, Integer> doNext() throws GeneratorException {
                return new Window.SingleItemWindow<Integer>();
            }
        };
        Iterator<Window<Integer, Integer>> windowGenerator = new WindowGenerator<Integer, Window<Integer, Integer>>(
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
        Iterator<Integer> identityGenerator = new IdentityGenerator<Integer>(numbers);

        // When
        Iterator<Window<Integer, Integer>> windows = new Generator<Window<Integer, Integer>>() {
            @Override
            protected Window<Integer, Integer> doNext() throws GeneratorException {
                return new Window.SingleItemWindow<Integer>();
            }
        };
        Iterator<Window<Integer, Integer>> windowGenerator = new WindowGenerator<Integer, Window<Integer, Integer>>(
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
        Iterator<Integer> identityGenerator = new IdentityGenerator<Integer>(numbers);

        // When
        final Window.SingleItemWindow window = new Window.SingleItemWindow<Integer>();
        Iterator<Window<Integer, Integer>> windows = new Generator<Window<Integer, Integer>>() {
            @Override
            protected Window<Integer, Integer> doNext() throws GeneratorException {
                window.reset();
                return window;
            }
        };
        Iterator<Window<Integer, Integer>> windowGenerator = new WindowGenerator<Integer, Window<Integer, Integer>>(
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
        Iterator<Integer> identityGenerator = new IdentityGenerator<Integer>(numbers);

        // When
        Iterator<Window<Integer, Integer>> windows = new Generator<Window<Integer, Integer>>() {
            @Override
            protected Window<Integer, Integer> doNext() throws GeneratorException {
                return new Window.SingleItemWindow<Integer>();
            }
        };
        Iterator<Window<Integer, Integer>> windowGenerator = new WindowGenerator<Integer, Window<Integer, Integer>>(
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
        Iterator<Integer> identityGenerator = new IdentityGenerator<Integer>(numbers);

        // When
        Iterator<Window<Integer, Integer>> windows = new Generator<Window<Integer, Integer>>() {
            @Override
            protected Window<Integer, Integer> doNext() throws GeneratorException {
                return new Window.SingleItemWindow<Integer>();
            }
        };
        Iterator<Window<Integer, Integer>> windowGenerator = new WindowGenerator<Integer, Window<Integer, Integer>>(
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