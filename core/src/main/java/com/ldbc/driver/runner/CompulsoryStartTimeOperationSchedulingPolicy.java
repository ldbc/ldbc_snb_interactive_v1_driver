package com.ldbc.driver.runner;

import org.apache.log4j.Logger;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationException;
import com.ldbc.driver.util.temporal.Duration;

public class CompulsoryStartTimeOperationSchedulingPolicy implements OperationSchedulingPolicy
{
    private static Logger logger = Logger.getLogger( CompulsoryStartTimeOperationSchedulingPolicy.class );

    private final Duration toleratedDelay;

    public CompulsoryStartTimeOperationSchedulingPolicy( Duration toleratedDelay )
    {
        this.toleratedDelay = toleratedDelay;
    }

    @Override
    public boolean ignoreScheduledStartTime()
    {
        return false;
    }

    @Override
    public void handleUnassignedScheduledStartTime() throws OperationException
    {
        String errMsg = String.format( "Operation must have an assigned Scheduled Start Time" );
        logger.error( errMsg );
        throw new OperationException( errMsg );
    }

    @Override
    public Duration toleratedDelay()
    {
        return toleratedDelay;
    }

    @Override
    public void handleExcessiveDelay( Operation<?> operation ) throws OperationException
    {
        String errMsg = String.format( "Tolerated scheduled start time delay (%s) exceeded on operation\n%s",
                toleratedDelay, operation );
        logger.error( errMsg );
        throw new OperationException( errMsg );
    }
}
