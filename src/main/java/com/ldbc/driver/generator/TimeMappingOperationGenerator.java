package com.ldbc.driver.generator;

import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.Function1;

import java.util.Iterator;

public class TimeMappingOperationGenerator extends Generator<Operation<?>> {
    private final Iterator<Operation<?>> operations;
    private final long newStartTimeAsMilli;
    private final Double timeCompressionRatio;

    private Function1<Long, Long> timeOffsetAsMilliFun = null;
    private Function1<Long, Long> startTimeAsMilliCompressionFun = null;
    private Function1<Long, Long> dependencyTimeAsMilliCompressionFun = null;

    TimeMappingOperationGenerator(Iterator<Operation<?>> operations, long newStartTimeAsMilli, Double timeCompressionRatio) {
        this.operations = operations;
        this.newStartTimeAsMilli = newStartTimeAsMilli;
        this.timeCompressionRatio = timeCompressionRatio;
    }

    @Override
    protected Operation<?> doNext() throws GeneratorException {
        if (false == operations.hasNext()) return null;
        Operation<?> nextOperation = operations.next();
        if (null == timeOffsetAsMilliFun) {
            // Create time offset function
            Time firstStartTime = nextOperation.scheduledStartTimeAsMilli();
            if (newStartTimeAsMilli.gt(firstStartTime)) {
                // offset to future
                Duration offset = newStartTimeAsMilli.durationGreaterThan(firstStartTime);
                timeOffsetAsMilliFun = new TimeFutureOffsetFun(offset);
            } else {
                // offset to past
                Duration offset = newStartTimeAsMilli.durationLessThan(firstStartTime);
                timeOffsetAsMilliFun = new TimePastOffsetFun(offset);
            }

            // Create time compression function
            if (null == timeCompressionRatio) {
                startTimeAsMilliCompressionFun = new IdentityTimeFun();
                dependencyTimeAsMilliCompressionFun = new IdentityTimeFun();
            } else {
                startTimeAsMilliCompressionFun = new TimeCompressionFun(timeCompressionRatio, timeOffsetAsMilliFun.apply(nextOperation.scheduledStartTimeAsMilli()));
                dependencyTimeAsMilliCompressionFun = new TimeCompressionFun(timeCompressionRatio, timeOffsetAsMilliFun.apply(nextOperation.dependencyTimeAsMilli()));
            }
        }
        Time offsetStartTime = timeOffsetAsMilliFun.apply(nextOperation.scheduledStartTimeAsMilli());
        Time offsetDependencyTime = timeOffsetAsMilliFun.apply(nextOperation.dependencyTimeAsMilli());
        Time offsetAndCompressedStartTime = startTimeAsMilliCompressionFun.apply(offsetStartTime);
        Time offsetAndCompressedDependencyTime = dependencyTimeAsMilliCompressionFun.apply(offsetDependencyTime);
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
