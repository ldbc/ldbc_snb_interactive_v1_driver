package org.ldbcouncil.snb.driver.validation;
/**
 * ResultsLogValidationTolerances.java
 * 
 */


public class ResultsLogValidationTolerances
{
    private final long excessiveDelayThresholdAsMilli;
    private final long toleratedExcessiveDelayCount;

    public ResultsLogValidationTolerances(
        long excessiveDelayThresholdAsMilli,
        long toleratedExcessiveDelayCount)
    {
        this.excessiveDelayThresholdAsMilli = excessiveDelayThresholdAsMilli;
        this.toleratedExcessiveDelayCount = toleratedExcessiveDelayCount;
    }

    public long excessiveDelayThresholdAsMilli()
    {
        return excessiveDelayThresholdAsMilli;
    }

    public long toleratedExcessiveDelayCount()
    {
        return toleratedExcessiveDelayCount;
    }
}
