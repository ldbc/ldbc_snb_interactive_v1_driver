package com.ldbc.driver.generator;

import com.google.common.base.Predicate;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandler;

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
        private final long windowStartTimeInclusiveAsMilli;
        private final long windowEndTimeExclusiveAsMilli;
        private final List<OperationHandler<?>> contents;
        private boolean isComplete;

        public OperationHandlerTimeRangeWindow(long windowStartTimeInclusiveAsMilli, long windowDurationAsMilli) {
            this.windowStartTimeInclusiveAsMilli = windowStartTimeInclusiveAsMilli;
            this.windowEndTimeExclusiveAsMilli = windowStartTimeInclusiveAsMilli + windowDurationAsMilli;
            this.contents = new ArrayList<>();
            this.isComplete = false;
        }

        @Override
        public boolean add(OperationHandler<?> operationHandler) {
            long startTimeAsMilli = operationHandler.operation().scheduledStartTimeAsMilli();
            if (startTimeAsMilli >= windowStartTimeInclusiveAsMilli && startTimeAsMilli < windowEndTimeExclusiveAsMilli) {
                return contents.add(operationHandler);
            }
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

        public long windowStartTimeAsMilliInclusive() {
            return windowStartTimeInclusiveAsMilli;
        }

        public long windowEndTimeAsMilliExclusive() {
            return windowEndTimeExclusiveAsMilli;
        }
    }

    public static class OperationTimeRangeWindow implements Window<Operation<?>, List<Operation<?>>> {
        private final long windowStartTimeAsMilliInclusive;
        private final long windowEndTimeAsMilliExclusive;
        private final List<Operation<?>> contents;
        private boolean isComplete;

        public OperationTimeRangeWindow(long windowStartTimeAsMilliInclusive, long windowDurationAsMilli) {
            this.windowStartTimeAsMilliInclusive = windowStartTimeAsMilliInclusive;
            this.windowEndTimeAsMilliExclusive = windowStartTimeAsMilliInclusive + windowDurationAsMilli;
            this.contents = new ArrayList<>();
            this.isComplete = false;
        }

        @Override
        public boolean add(Operation<?> operation) {
            long startTimeAsMilli = operation.scheduledStartTimeAsMilli();
            if (startTimeAsMilli >= windowStartTimeAsMilliInclusive && startTimeAsMilli < windowEndTimeAsMilliExclusive) {
                return contents.add(operation);
            }
            isComplete = true;
            return false;
        }

        @Override
        public List<Operation<?>> contents() {
            return contents;
        }

        @Override
        public boolean isComplete() {
            return isComplete;
        }

        public long windowStartTimeAsMilliInclusive() {
            return windowStartTimeAsMilliInclusive;
        }

        public long windowEndTimeAsMilliExclusive() {
            return windowEndTimeAsMilliExclusive;
        }
    }
}