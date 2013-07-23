package com.ldbc.driver.runner;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationException;
import com.ldbc.driver.util.temporal.Time;

public class Spinner
{
    private final OperationSchedulingPolicy operationSchedulingPolicy;

    public Spinner( OperationSchedulingPolicy lateOperationPolicy )
    {
        this.operationSchedulingPolicy = lateOperationPolicy;
    }

    public void waitForScheduledStartTime( Operation<?> operation ) throws OperationException
    {
        if ( operationSchedulingPolicy.ignoreScheduledStartTime() )
        {
            return;
        }

        if ( operation.getScheduledStartTime().equals( Operation.UNASSIGNED_SCHEDULED_START_TIME ) )
        {
            operationSchedulingPolicy.handleUnassignedScheduledStartTime();
        }

        if ( Time.now().greaterBy( operation.getScheduledStartTime() ).greatThan(
                operationSchedulingPolicy.toleratedDelay() ) )
        {
            operationSchedulingPolicy.handleExcessiveDelay( operation );
        }

        while ( Time.now().asNano() < operation.getScheduledStartTime().asNano() )
        {
            // loop/wait until operation scheduled start time
        }
    }
}
