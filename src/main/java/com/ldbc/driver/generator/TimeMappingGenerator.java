package com.ldbc.driver.generator;

import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.Function1;

import java.util.Iterator;

public class TimeMappingGenerator extends Generator<Operation<?>> {
    private final Iterator<Operation<?>> original;
    private final Time newStartTime;
    private final Double timeCompressionRatio;

    private Function1<Time, Time> timeOffsetFun = null;
    private Function1<Time, Time> timeCompressionFun = null;

    TimeMappingGenerator(Iterator<Operation<?>> original, Time newStartTime, Double timeCompressionRatio) {
        this.original = original;
        this.newStartTime = newStartTime;
        this.timeCompressionRatio = timeCompressionRatio;
    }

    @Override
    protected Operation<?> doNext() throws GeneratorException {
        if (false == original.hasNext()) return null;
        Operation<?> nextOperation = original.next();
        if (null == timeOffsetFun) {
            // Create timeOffsetFun
            Time firstStartTime = nextOperation.scheduledStartTime();
            if (newStartTime.gt(firstStartTime)) {
                // offset to future
                Duration offset = newStartTime.greaterBy(firstStartTime);
                timeOffsetFun = new TimeFutureOffsetFun(offset);
            } else {
                // offset to past
                Duration offset = newStartTime.lessBy(firstStartTime);
                timeOffsetFun = new TimePastOffsetFun(offset);
            }

            // Create timeCompressionFun
            if (null == timeCompressionRatio) {
                timeCompressionFun = new IdentityTimeFun();
            } else {
                timeCompressionFun = new TimeCompressionFun(timeCompressionRatio, newStartTime);
            }

            nextOperation.setScheduledStartTime(newStartTime);
            return nextOperation;
        } else {
            Time offsetTime = timeOffsetFun.apply(nextOperation.scheduledStartTime());
            Time compressedTime = timeCompressionFun.apply(offsetTime);
            nextOperation.setScheduledStartTime(compressedTime);
            return nextOperation;
        }
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
            long durationFromOriginalStartTimeAsNano = time.greaterBy(firstTime).asNano();
            long compressedDurationFromOriginalStartTimeIsNano = Math.round(durationFromOriginalStartTimeAsNano * timeCompressionRatio);
            Duration compressedDurationFromOriginalStartTime = Duration.fromNano(compressedDurationFromOriginalStartTimeIsNano);
            return firstTime.plus(compressedDurationFromOriginalStartTime);
        }
    }

}
