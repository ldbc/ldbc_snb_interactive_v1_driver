package com.ldbc.driver.generator;

import com.google.common.base.Predicate;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

import java.util.ArrayList;
import java.util.List;

public interface Window<INPUT_TYPE, RETURN_TYPE> {
    boolean add(INPUT_TYPE t);

    RETURN_TYPE contents();

    boolean isComplete();

    public static class SizeWindow<I> implements Window<I, List<I>> {
        private int windowSize;
        private List<I> contents;

        public SizeWindow(int windowSize) {
            this.windowSize = windowSize;
            this.contents = new ArrayList<I>();
        }

        @Override
        public boolean add(I input) {
            return (contents.size() < windowSize) && contents.add(input);
        }

        @Override
        public List<I> contents() {
            return contents;
        }

        @Override
        public boolean isComplete() {
            return contents.size() == windowSize;
        }
    }

    public static class PredicateWindow<I> implements Window<I, List<I>> {
        private Predicate<I> predicate;
        private List<I> contents;
        private boolean isComplete;

        public PredicateWindow(Predicate<I> predicate) {
            this.predicate = predicate;
            this.contents = new ArrayList<I>();
            this.isComplete = false;
        }

        @Override
        public boolean add(I input) {
            if (predicate.apply(input))
                return contents.add(input);
            isComplete = true;
            return false;
        }

        @Override
        public List<I> contents() {
            return contents;
        }

        @Override
        public boolean isComplete() {
            return isComplete;
        }
    }

    public static class SingleItemWindow<I> implements Window<I, I> {
        private I item = null;

        @Override
        public boolean add(I item) {
            if (null != this.item)
                return false;
            this.item = item;
            return true;
        }

        @Override
        public I contents() {
            return item;
        }

        @Override
        public boolean isComplete() {
            return item != null;
        }

        public void reset() {
            item = null;
        }
    }

    public static class OperationHandlerTimeRangeWindow implements Window<OperationHandler<?>, List<OperationHandler<?>>> {
        private final Time windowStartTimeInclusive;
        private final Time windowEndTimeExclusive;
        private final List<OperationHandler<?>> contents;
        private boolean isComplete;

        public OperationHandlerTimeRangeWindow(Time windowStartTimeInclusive, Duration windowDuration) {
            this.windowStartTimeInclusive = windowStartTimeInclusive;
            this.windowEndTimeExclusive = windowStartTimeInclusive.plus(windowDuration);
            this.contents = new ArrayList<>();
            this.isComplete = false;
        }

        @Override
        public boolean add(OperationHandler<?> handler) {
            if (handler.operation().scheduledStartTime().gte(windowStartTimeInclusive) &&
                    handler.operation().scheduledStartTime().lt(windowEndTimeExclusive))
                return contents.add(handler);
            isComplete = true;
            return false;
        }

        @Override
        public List<OperationHandler<?>> contents() {
            return contents;
        }

        @Override
        public boolean isComplete() {
            return isComplete;
        }

        public Time windowStartTimeInclusive() {
            return windowStartTimeInclusive;
        }

        public Time windowEndTimeExclusive() {
            return windowEndTimeExclusive;
        }
    }
}