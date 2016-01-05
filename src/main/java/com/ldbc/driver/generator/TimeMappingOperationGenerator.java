package com.ldbc.driver.generator;

import com.ldbc.driver.Operation;
import com.ldbc.driver.util.Function1;

import java.util.Iterator;

public class TimeMappingOperationGenerator extends Generator<Operation>
{
    private final Iterator<Operation> operations;
    private final long newStartTimeAsMilli;
    private final Double timeCompressionRatio;

    private Function1<Long,Long,RuntimeException> timeOffsetAsMilliFun = null;
    private Function1<Long,Long,RuntimeException> startTimeAsMilliCompressionFun = null;

    TimeMappingOperationGenerator(
            Iterator<Operation> operations,
            long newStartTimeAsMilli,
            Double timeCompressionRatio )
    {
        this.operations = operations;
        this.newStartTimeAsMilli = newStartTimeAsMilli;
        this.timeCompressionRatio = timeCompressionRatio;
    }

    @Override
    protected Operation doNext() throws GeneratorException
    {
        if ( false == operations.hasNext() )
        { return null; }
        Operation nextOperation = operations.next();
        if ( null == timeOffsetAsMilliFun )
        {
            // Create time offset function
            long firstStartTimeAsMilli = nextOperation.scheduledStartTimeAsMilli();
            if ( newStartTimeAsMilli > firstStartTimeAsMilli )
            {
                // offset to future
                long offsetAsMilli = newStartTimeAsMilli - firstStartTimeAsMilli;
                timeOffsetAsMilliFun = new TimeFutureOffsetFun( offsetAsMilli );
            }
            else
            {
                // offset to past
                long offsetAsMilli = firstStartTimeAsMilli - newStartTimeAsMilli;
                timeOffsetAsMilliFun = new TimePastOffsetFun( offsetAsMilli );
            }

            // Create time compression function
            if ( null == timeCompressionRatio )
            {
                startTimeAsMilliCompressionFun = new IdentityTimeFun();
            }
            else
            {
                startTimeAsMilliCompressionFun = new TimeCompressionFun(
                        timeCompressionRatio,
                        timeOffsetAsMilliFun.apply( nextOperation.scheduledStartTimeAsMilli() )
                );
            }
        }
        long offsetStartTimeAsMilli = timeOffsetAsMilliFun.apply( nextOperation.scheduledStartTimeAsMilli() );
        long offsetAndCompressedStartTimeAsMilli = startTimeAsMilliCompressionFun.apply( offsetStartTimeAsMilli );
        nextOperation.setScheduledStartTimeAsMilli( offsetAndCompressedStartTimeAsMilli );
        return nextOperation;
    }

    private class IdentityTimeFun implements Function1<Long,Long,RuntimeException>
    {
        @Override
        public Long apply( Long timeAsMilli )
        {
            return timeAsMilli;
        }
    }

    private class TimeFutureOffsetFun implements Function1<Long,Long,RuntimeException>
    {
        private final long offsetAsMilli;

        private TimeFutureOffsetFun( long offsetAsMilli )
        {
            this.offsetAsMilli = offsetAsMilli;
        }

        @Override
        public Long apply( Long timeAsMilli )
        {
            return timeAsMilli + offsetAsMilli;
        }
    }

    private class TimePastOffsetFun implements Function1<Long,Long,RuntimeException>
    {
        private final long offsetAsMilli;

        private TimePastOffsetFun( long offsetAsMilli )
        {
            this.offsetAsMilli = offsetAsMilli;
        }

        @Override
        public Long apply( Long timeAsMilli )
        {
            return timeAsMilli - offsetAsMilli;
        }
    }

    private class TimeCompressionFun implements Function1<Long,Long,RuntimeException>
    {
        private final double timeCompressionRatio;
        private long firstTimeAsMilli;

        private TimeCompressionFun( double timeCompressionRatio, long firstTimeAsMilli )
        {
            this.timeCompressionRatio = timeCompressionRatio;
            this.firstTimeAsMilli = firstTimeAsMilli;
        }

        @Override
        public Long apply( Long timeAsMilli )
        {
            long durationFromOriginalStartTimeAsMilli = timeAsMilli - firstTimeAsMilli;
            long compressedDurationFromOriginalStartTimeAsMilli =
                    Math.round( durationFromOriginalStartTimeAsMilli * timeCompressionRatio );
            return firstTimeAsMilli + compressedDurationFromOriginalStartTimeAsMilli;
        }
    }

}
