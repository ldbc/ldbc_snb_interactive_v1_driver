package com.ldbc.driver.validation;

import java.util.Map;

public class ResultsLogValidationTolerances
{
    private final long excessiveDelayThresholdAsMilli;
    private final long toleratedExcessiveDelayCount;
    private final Map<String,Long> toleratedExcessiveDelayCountPerType;

    public ResultsLogValidationTolerances(
            long excessiveDelayThresholdAsMilli,
            long toleratedExcessiveDelayCount,
            Map<String,Long> toleratedExcessiveDelayCountPerType )
    {
        this.excessiveDelayThresholdAsMilli = excessiveDelayThresholdAsMilli;
        this.toleratedExcessiveDelayCount = toleratedExcessiveDelayCount;
        this.toleratedExcessiveDelayCountPerType = toleratedExcessiveDelayCountPerType;
    }

    public long excessiveDelayThresholdAsMilli()
    {
        return excessiveDelayThresholdAsMilli;
    }

    public long toleratedExcessiveDelayCount()
    {
        return toleratedExcessiveDelayCount;
    }

    public Map<String,Long> toleratedExcessiveDelayCountPerType()
    {
        return toleratedExcessiveDelayCountPerType;
    }
}
