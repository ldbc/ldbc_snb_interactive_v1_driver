package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.ManualTimeSource;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.workloads.dummy.NothingOperation;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import org.junit.Ignore;
import org.junit.Test;

import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SpinnerTests
{
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
    long ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING = 500;
    ManualTimeSource timeSource = new ManualTimeSource( 0 );
    DecimalFormat integerFormat = new DecimalFormat( "###,###,###,###,###" );

    @Test
    public void shouldPassWhenNoCheckAndStartTimeArrives() throws InterruptedException
    {
        // Given
        timeSource.setNowFromMilli( 0 );
        boolean ignoreScheduledStartTime = false;
        long spinnerSleepDuration = 0l;
        Spinner spinner = new Spinner( timeSource, spinnerSleepDuration, ignoreScheduledStartTime );

        long scheduledStartTime = 10l;
        Operation operation = new TimedNamedOperation1( scheduledStartTime, scheduledStartTime, 0l, "name" );

        SpinningThread spinningThread = new SpinningThread( spinner, operation );

        // When
        spinningThread.start();

        // Then
        // should not return before start time
        Thread.sleep( ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING );
        assertThat( spinningThread.spinnerHasCompleted(), is( false ) );
        assertThat( spinningThread.isFineToExecuteOperation(), is( false ) );

        timeSource.setNowFromMilli( scheduledStartTime );

        // should return when start time reached
        Thread.sleep( ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING );
        assertThat( spinningThread.spinnerHasCompleted(), is( true ) );
        assertThat( spinningThread.isFineToExecuteOperation(), is( true ) );

        spinningThread.join( ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING );
    }

    @Test
    public void shouldPassOnlyWhenCheckPassesAndStartTimeArrives() throws InterruptedException
    {
        // Given
        timeSource.setNowFromMilli( 0 );
        boolean ignoreScheduledStartTime = false;
        long spinnerSleepDuration = 0l;
        SettableSpinnerCheck check = new SettableSpinnerCheck( SpinnerCheck.SpinnerCheckResult.STILL_CHECKING );
        Spinner spinner = new Spinner( timeSource, spinnerSleepDuration, ignoreScheduledStartTime );

        long scheduledStartTime = 10l;
        Operation operation = new TimedNamedOperation1( scheduledStartTime, scheduledStartTime, 0l, "name" );

        SpinningThread spinningThread = new SpinningThread( spinner, operation, check );

        // When
        spinningThread.start();

        // Then
        // time = no, check = not yet
        Thread.sleep( ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING );
        assertThat( spinningThread.spinnerHasCompleted(), is( false ) );
        assertThat( spinningThread.isFineToExecuteOperation(), is( false ) );

        timeSource.setNowFromMilli( scheduledStartTime - 1 );

        // time = no, check = not yet
        Thread.sleep( ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING );
        assertThat( spinningThread.spinnerHasCompleted(), is( false ) );
        assertThat( spinningThread.isFineToExecuteOperation(), is( false ) );

        timeSource.setNowFromMilli( scheduledStartTime );

        // time = yes, check = not yet
        Thread.sleep( ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING );
        assertThat( spinningThread.spinnerHasCompleted(), is( false ) );
        assertThat( spinningThread.isFineToExecuteOperation(), is( false ) );

        check.setResult( SpinnerCheck.SpinnerCheckResult.PASSED );

        // time = yes, check = yes
        Thread.sleep( ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING );
        assertThat( spinningThread.spinnerHasCompleted(), is( true ) );
        assertThat( spinningThread.isFineToExecuteOperation(), is( true ) );

        spinningThread.join( ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING );
    }

    @Test
    public void shouldFailWhenCheckFails() throws InterruptedException
    {
        // Given
        timeSource.setNowFromMilli( 0 );
        boolean ignoreScheduledStartTime = false;
        long spinnerSleepDuration = 0l;
        SettableSpinnerCheck check = new SettableSpinnerCheck( SpinnerCheck.SpinnerCheckResult.STILL_CHECKING );
        Spinner spinner = new Spinner( timeSource, spinnerSleepDuration, ignoreScheduledStartTime );

        long scheduledStartTime = 10l;
        Operation operation = new TimedNamedOperation1( scheduledStartTime, scheduledStartTime, 0l, "name" );

        SpinningThread spinningThread = new SpinningThread( spinner, operation, check );

        // When
        spinningThread.start();

        // Then
        // time = no, check = not yet
        Thread.sleep( ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING );
        assertThat( spinningThread.spinnerHasCompleted(), is( false ) );
        assertThat( spinningThread.isFineToExecuteOperation(), is( false ) );

        timeSource.setNowFromMilli( scheduledStartTime - 1 );

        // time = no, check = not yet
        Thread.sleep( ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING );
        assertThat( spinningThread.spinnerHasCompleted(), is( false ) );
        assertThat( spinningThread.isFineToExecuteOperation(), is( false ) );

        timeSource.setNowFromMilli( scheduledStartTime );

        // time = yes, check = not yet
        Thread.sleep( ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING );
        assertThat( spinningThread.spinnerHasCompleted(), is( false ) );
        assertThat( spinningThread.isFineToExecuteOperation(), is( false ) );

        check.setResult( SpinnerCheck.SpinnerCheckResult.FAILED );

        // time = yes, check = no
        Thread.sleep( ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING );
        assertThat( spinningThread.spinnerHasCompleted(), is( true ) );
        assertThat( spinningThread.isFineToExecuteOperation(), is( false ) );

        spinningThread.join( ENOUGH_MILLISECONDS_FOR_SPINNER_THREAD_TO_DO_ITS_THING );
    }

    private static class SpinningThread extends Thread
    {
        private final Spinner spinner;
        private final Operation operation;
        private final AtomicBoolean isFineToExecuteOperation;
        private final AtomicBoolean spinnerHasCompleted;
        private final SpinnerCheck check;

        SpinningThread( Spinner spinner, Operation operation )
        {
            this( spinner, operation, null );
        }

        SpinningThread( Spinner spinner, Operation operation, SpinnerCheck check )
        {
            this.spinner = spinner;
            this.operation = operation;
            this.check = check;
            this.spinnerHasCompleted = new AtomicBoolean( false );
            this.isFineToExecuteOperation = new AtomicBoolean( false );
        }

        @Override
        public void run()
        {
            try
            {
                if ( null == check )
                { isFineToExecuteOperation.set( spinner.waitForScheduledStartTime( operation ) ); }
                else
                { isFineToExecuteOperation.set( spinner.waitForScheduledStartTime( operation, check ) ); }
                spinnerHasCompleted.set( true );
            }
            catch ( Throwable e )
            {
                e.printStackTrace();
            }
        }

        boolean spinnerHasCompleted()
        {
            return spinnerHasCompleted.get();
        }

        boolean isFineToExecuteOperation()
        {
            return isFineToExecuteOperation.get();
        }
    }

    // This testing methodology seems bad, not enough iterations or something, the numbers are dependent on order
    // things are done
    @Ignore
    @Test
    public void measureCostOfSpinnerWithNoSleepAndPassingCheckAndAtScheduledStartTime()
    {
        TimeSource systemTimeSource = new SystemTimeSource();
        timeSource.setNowFromMilli( 0 );
        long scheduledStartTime = this.timeSource.nowAsMilli();
        long operationCount = 100000000;
        int experimentCount = 10;
        boolean ignoreScheduledStartTime;

        ignoreScheduledStartTime = false;
        Spinner spinnerWithStartTimeCheck = new Spinner( timeSource, 0l, ignoreScheduledStartTime );
        SpinnerCheck singleTrueCheck = new SettableSpinnerCheck( SpinnerCheck.SpinnerCheckResult.PASSED );

        long singleCheckWithStartTimeCheckTestDuration = 0l;
        long singleCheckWithoutStartTimeCheckTestDuration = 0l;

        for ( int i = 0; i < experimentCount; i++ )
        {
            FastSameOperationIterator operationsSingleCheckWithStartTimeCheck =
                    new FastSameOperationIterator( scheduledStartTime, operationCount );
            long singleCheckWithStartTimeCheckTestStartTime = systemTimeSource.nowAsMilli();
            while ( operationsSingleCheckWithStartTimeCheck.hasNext() )
            {
                spinnerWithStartTimeCheck
                        .waitForScheduledStartTime( operationsSingleCheckWithStartTimeCheck.next(), singleTrueCheck );
            }
            singleCheckWithStartTimeCheckTestDuration =
                    singleCheckWithStartTimeCheckTestDuration +
                    (systemTimeSource.nowAsMilli() - singleCheckWithStartTimeCheckTestStartTime);

            FastSameOperationIterator operationsSingleCheckWithoutStartTimeCheck =
                    new FastSameOperationIterator( scheduledStartTime, operationCount );
            long singleCheckWithoutStartTimeCheckTestStartTime = systemTimeSource.nowAsMilli();
            while ( operationsSingleCheckWithoutStartTimeCheck.hasNext() )
            {
                spinnerWithStartTimeCheck.waitForScheduledStartTime( operationsSingleCheckWithoutStartTimeCheck.next(),
                        singleTrueCheck );
            }
            singleCheckWithoutStartTimeCheckTestDuration =
                    singleCheckWithoutStartTimeCheckTestDuration +
                    (systemTimeSource.nowAsMilli() - singleCheckWithoutStartTimeCheckTestStartTime);
        }

        singleCheckWithStartTimeCheckTestDuration = singleCheckWithStartTimeCheckTestDuration / experimentCount;
        singleCheckWithoutStartTimeCheckTestDuration = singleCheckWithoutStartTimeCheckTestDuration / experimentCount;

        System.out.println(
                format( "Spinner(start time check = true) (1 true check) processed %s operations in %s: %s ops/ms, %s" +
                        " ns/op",
                        operationCount,
                        TEMPORAL_UTIL.milliDurationToString( singleCheckWithStartTimeCheckTestDuration ),
                        integerFormat.format( operationCount / singleCheckWithStartTimeCheckTestDuration ),
                        TimeUnit.MILLISECONDS.toNanos( singleCheckWithStartTimeCheckTestDuration ) / operationCount ) );

        System.out.println(
                format( "Spinner(start time check = false) (1 true check) processed %s operations in %s: %s ops/ms, " +
                        "%s ns/op",
                        operationCount,
                        TEMPORAL_UTIL.milliDurationToString( singleCheckWithoutStartTimeCheckTestDuration ),
                        integerFormat.format( operationCount / singleCheckWithoutStartTimeCheckTestDuration ),
                        TimeUnit.MILLISECONDS.toNanos( singleCheckWithoutStartTimeCheckTestDuration ) /
                        operationCount ) );
    }

    private static class FastSameOperationIterator implements Iterator<Operation>
    {
        private final Operation operation;
        private long currentOperationCount = 0;
        private final long operationCount;

        FastSameOperationIterator( long scheduledStartTime, long operationCount )
        {
            operation = new NothingOperation();
            operation.setScheduledStartTimeAsMilli( scheduledStartTime );
            operation.setTimeStamp( scheduledStartTime );
            this.operationCount = operationCount;
        }

        @Override
        public boolean hasNext()
        {
            return currentOperationCount < operationCount;
        }

        @Override
        public Operation next()
        {
            currentOperationCount++;
            return operation;
        }

        @Override
        public void remove()
        {
        }
    }
}
