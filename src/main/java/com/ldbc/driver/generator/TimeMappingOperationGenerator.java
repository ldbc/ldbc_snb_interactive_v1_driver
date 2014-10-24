package com.ldbc.driver.generator;

import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.Function1;

import java.util.Iterator;

public class TimeMappingOperationGenerator extends Generator<Operation<?>> {
    private final Iterator<Operation<?>> operations;
    private final Time newStartTime;
    private final Double timeCompressionRatio;

    private Function1<Time, Time> timeOffsetFun = null;
    private Function1<Time, Time> startTimeCompressionFun = null;
    private Function1<Time, Time> dependencyTimeCompressionFun = null;

    TimeMappingOperationGenerator(Iterator<Operation<?>> operations, Time newStartTime, Double timeCompressionRatio) {
        this.operations = operations;
        this.newStartTime = newStartTime;
        this.timeCompressionRatio = timeCompressionRatio;
    }

    @Override
    protected Operation<?> doNext() throws GeneratorException {
        if (false == operations.hasNext()) return null;
        Operation<?> nextOperation = operations.next();
        if (null == timeOffsetFun) {
            // Create timeOffsetFun
            Time firstStartTime = nextOperation.scheduledStartTimeAsMilli();
            if (newStartTime.gt(firstStartTime)) {
                // offset to future
                Duration offset = newStartTime.durationGreaterThan(firstStartTime);
                timeOffsetFun = new TimeFutureOffsetFun(offset);
            } else {
                // offset to past
                Duration offset = newStartTime.durationLessThan(firstStartTime);
                timeOffsetFun = new TimePastOffsetFun(offset);
            }

            // Create time compression function
            if (null == timeCompressionRatio) {
                startTimeCompressionFun = new IdentityTimeFun();
                dependencyTimeCompressionFun = new IdentityTimeFun();
            } else {
                startTimeCompressionFun = new TimeCompressionFun(timeCompressionRatio, timeOffsetFun.apply(nextOperation.scheduledStartTimeAsMilli()));
                dependencyTimeCompressionFun = new TimeCompressionFun(timeCompressionRatio, timeOffsetFun.apply(nextOperation.dependencyTimeAsMilli()));
            }
        }
        Time offsetStartTime = timeOffsetFun.apply(nextOperation.scheduledStartTimeAsMilli());
        Time offsetDependencyTime = timeOffsetFun.apply(nextOperation.dependencyTimeAsMilli());
        Time offsetAndCompressedStartTime = startTimeCompressionFun.apply(offsetStartTime);
        Time offsetAndCompressedDependencyTime = dependencyTimeCompressionFun.apply(offsetDependencyTime);
        nextOperation.setScheduledStartTimeAsMilli(offsetAndCompressedStartTime);
        nextOperation.setDependencyTimeAsMilli(offsetAndCompressedDependencyTime);
        return nextOperation;
    }

    private class IdentityTimeFun implements Function1<Time, Time> {
        @Override
        public Time apply(Time time) {
            return time;
        }
    }

    private class TimeFutureOffsetFun implements Function1<Time, Time> {
        private final Duration offset;

        private TimeFutureOffsetFun(Duration offset) {
            this.offset = offset;
        }

        @Override
        public Time apply(Time time) {
            return time.plus(offset);
        }
    }

    private class TimePastOffsetFun implements Function1<Time, Time> {
        private final Duration offset;

        private TimePastOffsetFun(Duration offset) {
            this.offset = offset;
        }

        @Override
        public Time apply(Time time) {
            return time.minus(offset);
        }
    }

    private class TimeCompressionFun implements Function1<Time, Time> {
        private final Double timeCompressionRatio;
        private Time firstTime;

        private TimeCompressionFun(Double timeCompressionRatio, Time firstTime) {
            this.timeCompressionRatio = timeCompressionRatio;
            this.firstTime = firstTime;
        }

        @Override
        public Time apply(Time time) {
            long durationFromOriginalStartTimeAsNano = time.durationGreaterThan(firstTime).asNano();
            long compressedDurationFromOriginalStartTimeIsNano = Math.round(durationFromOriginalStartTimeAsNano * timeCompressionRatio);
            Duration compressedDurationFromOriginalStartTime = Duration.fromNano(compressedDurationFromOriginalStartTimeIsNano);
            return firstTime.plus(compressedDurationFromOriginalStartTime);
        }
    }

}
