package org.ldbcouncil.snb.driver;
/**
 * Operation.java
 * 
 * Describes Operation performed by the driver, e.g. queries.
 * Each Operation is expected to have a RESULT_TYPE, which is the expected
 * result of the operation. The operation must be able to deserialize the
 * result object.
 */
import org.ldbcouncil.snb.driver.temporal.TemporalUtil;

import java.io.IOException;
import java.util.Map;

import org.ldbcouncil.snb.driver.Operation;

public abstract class Operation<RESULT_TYPE>
{
    private static final TemporalUtil temporalutil = new TemporalUtil();
    private long scheduledStartTimeAsMilli = -1;
    private long timeStamp = -1;
    private long expiryTimeStamp = -1;
    private long dependencyTimeStamp = -1;

    public final void setScheduledStartTimeAsMilli( long scheduledStartTimeAsMilli )
    {
        this.scheduledStartTimeAsMilli = scheduledStartTimeAsMilli;
    }

    public final void setDependencyTimeStamp( long dependencyTimeStamp )
    {
        this.dependencyTimeStamp = dependencyTimeStamp;
    }

    public final void setExpiryTimeStamp( long expiryTimeStamp )
    {
        this.expiryTimeStamp = expiryTimeStamp;
    }

    public final long scheduledStartTimeAsMilli()
    {
        return scheduledStartTimeAsMilli;
    }

    public final long dependencyTimeStamp()
    {
        return dependencyTimeStamp;
    }

    public long expiryTimeStamp()
    {
        return expiryTimeStamp;
    }

    public long timeStamp()
    {
        return timeStamp;
    }

    public void setTimeStamp( long timeStamp )
    {
        this.timeStamp = timeStamp;
    }

    /**
     * Get type of operation e.g.
     * @see org.ldbcouncil.snb.driver.workloads.interactive.LdbcSnbInteractiveWorkloadConfiguration#operationTypeToClassMapping()
     * for mapping of LDBC queries
     * @return Type as integer
     */
    public abstract int type();

    @Override
    public String toString()
    {
        return "Operation{" +
               "scheduledStartTime=" + temporalutil.milliTimeToDateTimeString( scheduledStartTimeAsMilli ) +
               ", timeStamp=" + temporalutil.milliTimeToDateTimeString( timeStamp ) +
               ", dependencyTimeStamp=" + temporalutil.milliTimeToDateTimeString( dependencyTimeStamp ) +
               '}';
    }

    /**
     * Maps the operation variable of an operation to the getter function
     * @return Map with operation variable as key and name of getter function
     */
    public abstract Map<String, Object> parameterMap();

    public Operation newInstance(){
        throw new UnsupportedOperationException();
    }

    /**
     * Deserializes a list of result objects
     * @param serializedOperationResult The serialized result object in a list
     * @return Deserialized result object
     * @throws IOException in case the given string cannot be deserialized to the result type
     */
    public abstract RESULT_TYPE deserializeResult( String serializedOperationResult) throws IOException;
}
